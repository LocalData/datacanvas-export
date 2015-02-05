'use strict';

var config = module.exports;

config.postgres = process.env.DATABASE_URL;

config.sources = process.env.SOURCES.split(',');

config.tmpDir = process.env.TMP_DIR || '/tmp';

config.awsKey = process.env.AWS_KEY;
config.awsSecret = process.env.AWS_SECRET;
config.bucket = process.env.S3_BUCKET;
config.uploadPath = process.env.S3_UPLOAD_PATH;
