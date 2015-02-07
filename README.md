Incrementally export LocalData Sensor API raw entries to a ZIP archive of CSVs

## Overview
See `sample.env`, or below, for necessary configuration values. If no file is found at the upload path, a full export is performed. That's a lot of database activity over otherwise quiet portions of the entries table, so you ought to perform any initial or fresh exports against a fork of the primary DB. If a source was present in the old export but is not in the current list of sources to process, that CSV will be left out of the new export. If you later add the source back in, that will also result in a fair amount of activity over old entries.

Each CSV in the zip file is named after the source ID. The `location` field from the raw entries is broken out into `longitude` and `latitude` fields.

## Running

`node lib/app.js`

To run locally with an appropriate environment, use [envrun](https://www.npmjs.com/package/envrun) or Foreman or similar.

## Configuration

Environment variables used for configuration:

+ `DATABASE_URL`: PostgreSQL connection URL
+ `PGSSLMODE`: Set to `require` if you need to use SSL to connect to postgres
+ `SOURCES`: comma-separated list of IDs, indicating which sources to process
+ `AWS_KEY`: AWS key ID
+ `AWS_SECRET`: AWS  secret key
+ `S3_BUCKET`: S3 bucket to use for storing (and retrieving) the export
+ `S3_UPLOAD_PATH`: path within the bucket to use for the export

The AWS user must have permission to get and put objects at that path in the bucket.

## TODO
+ Move the hardcoded list of headers into an environment variable.
