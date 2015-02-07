'use strict';

var fs = require('fs');

var _ = require('lodash');
var archiver = require('archiver');
var csv = require('csv-parser');
var csvWriter = require('csv-write-stream');
var knox = require('knox');
var logfmt = require('logfmt');
var Promise = require('bluebird');
var through2 = require('through2');
var unzip = require('node-unzip-2');

var config = require('./config');
var knex = require('./knex');

var s3Client = knox.createClient({
  key: config.awsKey,
  secret: config.awsSecret,
  bucket: config.bucket
});

var tmpDir = config.tmpDir;

Promise.promisifyAll(s3Client);

var sources = config.sources;

function promisifyStreamEnd(stream) {
  return new Promise(function (resolve, reject) {
    stream.on('error', reject);
    stream.on('end', resolve);
    stream.on('finish', resolve);
  });
}

function getLastTimestamp(input, done) {
  return new Promise(function (resolve, reject) {
    var last = 0;
    input.pipe(csv()).on('data', function (data) {
      last = data.timestamp;
    }).on('finish', function () {
      resolve(new Date(last));
    }).on('error', reject);
  }).nodeify(done);
}

// Keep track of how many rows we process.
var rowCount = 0;

// Based on the last timestamp, stream newer data via knex.
function getLatestEntries(source, timestamp) {
  return knex
  .select('ts as timestamp', 'source', 'data')
  .from('entries')
  .where('source', source)
  .andWhere('ts', '>', timestamp)
  .orderBy('ts', 'asc')
  .stream()
  .pipe(through2.obj(function (item, enc, done) {
    rowCount += 1;
    item.timestamp = item.timestamp.toISOString();
    _.defaults(item, item.data);
    item.data = undefined;
    item.longitude = item.location[0];
    item.latitude = item.location[1];
    item.location = undefined;
    this.push(item);
    done();
  }));
}

var headers = [
  'timestamp',
  'source',
  'longitude',
  'latitude',
  'temperature',
  'humidity',
  'light',
  'dust',
  'airquality_raw'
];

// Create a new ZIP file for output
var output = fs.createWriteStream(tmpDir + '/new.zip');
var archive = archiver('zip');
archive.pipe(output);
archive.on('error', function (error) {
  logfmt.error(error);
  throw error;
});

var oldEntryProgress = Promise.resolve();

function handleOldZipEntry(oldCsv) {
  // if the file isn't in the source list, skip. otherwise, stream to the new
  // zip file, but don't end the write stream when the read stream ends, since
  // we'll likely append more data.
  var id = oldCsv.path.slice(0, -4);
  if (_.includes(sources, id)) {
    // After we finish piping the old csv file to the new zip archive, we might
    // still have plenty of async work to do. So we don't proceed with an entry
    // until we're sure we've finished with the last one.
    oldEntryProgress = oldEntryProgress.then(function () {
      // Use a simple through stream, since we need to pass a stream into
      // archive.append, rather using pipe. This lets us switch the input to the
      // pipe.
      var passThrough = through2();
      archive.append(passThrough, { name: oldCsv.path });

      // Send the old entries to the new archive, but don't let the passThrough
      // stream end when we're done reading the old entries.
      oldCsv.pipe(passThrough, { end: false });

      // Record the last timestamp, and indicate when the old stream is done
      return getLastTimestamp(oldCsv)
      .then(function (ts) {
        logfmt.log({ op: 'copy', status: 'complete', source: id });
        // Remove this source from the list.
        sources = _.difference(sources, [id]);
        // Fetch new data from the database starting after the last timestamp.
        return promisifyStreamEnd(getLatestEntries(id, ts).pipe(csvWriter({
          headers: headers,
          sendHeaders: false
        })).pipe(passThrough)).then(function () {
          logfmt.log({ op: 'incremental-fetch', status: 'complete', source: id });
        });
      });
    });
  } else {
    // If we've removed this source from the list, then we don't want to keep
    // the old data around.
    oldCsv.autodrain();
  }
}

// Fetch latest ZIP file
s3Client.getFileAsync(config.uploadPath)
.then(function (res) {
  if (res.statusCode !== 200 && res.statusCode !== 404) {
    throw new Error('Received unexpected status code from S3: ' + res.statusCode);
  }
  logfmt.log({ op: 'fetch-old-archive', status: 'complete', statusCode: res.statusCode });
  if (res.statusCode === 200) {
    // Write it to a temporary file.
    var resPromise = promisifyStreamEnd(res);
    var tmpFileStream =res.pipe(fs.createWriteStream(tmpDir + '/old.zip'));
    return Promise.join(
      resPromise,
      promisifyStreamEnd(tmpFileStream)
    ).then(function () {
      // Pipe the old zip file to unzip, so we can process the files.
      var zipReadStream = fs.createReadStream(tmpDir + '/old.zip').pipe(unzip.Parse());
      zipReadStream.on('entry', handleOldZipEntry);
      return promisifyStreamEnd(zipReadStream);
    });
  }
}).then(function () {
  // After iterating over all the old files, check the sources list, in case
  // we've added new sources since the last export.

  var newSourceResolvers = {};

  // Handle completion of each fresh entry in the new archive.
  // TODO: Since we have the input streams for these entries, can we rely on the
  // end of those streams to determine that the archive is ready to be
  // finalized?
  archive.on('entry', function (entry) {
    var source = entry.name.slice(0,-4);
    var resolve = newSourceResolvers[source];
    // Make sure we only handle events from the brand new files.
    if (resolve) {
      resolve();
      newSourceResolvers[source] = null;
    }
  });

  return oldEntryProgress.then(function () {
    return sources;
  }).map(function (id) {
    var passThrough = through2();
    // For remaining sources, create a new file in the zip, write the header,
    // stream data via knex
    logfmt.log({ op: 'full-fetch', status: 'started', source: id });
    getLatestEntries(id, new Date(0))
    .pipe(csvWriter({ headers: headers }))
    .pipe(passThrough);
    archive.append(passThrough, { name: id + '.csv' });

    return promisifyStreamEnd(passThrough)
    .then(function () {
      logfmt.log({ op: 'full-fetch', status: 'complete', source: id });
      return new Promise(function (resolve) {
        // Resolved in archive's entry event
        newSourceResolvers[id] = resolve;
      });
    });
  }, {
    concurrency: 2
  }).then(function () {
    archive.finalize();
  });
}).then(function () {
  // Wait until the new file has been fully written.
  return promisifyStreamEnd(output);
}).then(function () {
  // Put the file on S3.
  logfmt.log({ op: 'upload', status: 'started', location: config.uploadPath });
  return s3Client.putFileAsync('/tmp/new.zip', config.uploadPath);
}).then(function (res) {
  res.resume();
  logfmt.log({ op: 'upload', status: 'complete', location: config.uploadPath });
  logfmt.log({ new_count: rowCount });
}).catch(function (error) {
  logfmt.error(error);
}).finally(function () {
  // Make sure we disconnect from postgres
  knex.destroy().catch(logfmt.error.bind(logfmt));
});
