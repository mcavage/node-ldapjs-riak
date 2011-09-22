// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');

var ldap = require('ldapjs');

var common = require('./common');



///--- Globals

var operationsError = common.operationsError;



///--- Handlers


/*
This is preferable, but currently blocked on Riak returning variable data from
a M/R job
function childExists(req, res, next) {
  var bucket = req.riak.bucket;
  var client = req.riak.client;
  var key = req.riak.key;
  var log = req.riak.log;

  log.debug('%s looking for children of %s', req.logId, key);
  var inputs = {
    bucket: req.riak.bucket,
    key_filters: [['ends_with', ', ' + key]]
  };
  var query =[
    {
      map: {
        language: 'erlang',
        module: 'riak_kv_mapreduce',
        'function': 'map_object_value'
      }
    },
    {
      reduce: {
        language: 'erlang',
        module: 'riak_kv_mapreduce',
        'function': 'reduce_identity',
        keep: true
      }
    }
  ];

  return client.mapred(inputs, query, function(err, data) {
    if (err)
      return next(operationsError(err));

    var count = (data && data.length) ? data.length : 0;

    log.debug('%s %s has %d children', req.logId, key, count);
    if (count)
      return next(new ldap.NotAllowedOnNonLeafError(key +
                                                    ' has ' + count +
                                                    ' child entries'));

    return next();
  });
}
*/


function del(req, res, next) {
  var bucket = req.riak.bucket;
  var client = req.riak.client;
  var key = req.riak.key;
  var log = req.riak.log;

  log.debug('%s removing %s', req.logId, key);
  return client.del(bucket, key, function(err) {
    if (err)
      return next(operationsError(err));

    log.debug('%s removed %s', req.logId, key);
    return next();
  });
}



///--- Exported API

module.exports = {

  chain: function(handlers) {
    assert.ok(handlers);

    [
      common.load,
      common.childExists,
      del,
      common.buildIndexKeys,
      common.unindex,
      common.done
    ].forEach(function(h) {
       handlers.push(h);
     });

    return handlers;
  }

};
