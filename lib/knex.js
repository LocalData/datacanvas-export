'use strict';

var knex = require('knex');

var config = require('./config');

module.exports = knex({
  client: 'pg',
  connection: config.postgres,
  pool: {
    min: 0,
    max: 2
  }
});
