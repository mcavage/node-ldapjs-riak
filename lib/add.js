// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');

var ldap = require('ldapjs');

var common = require('./common');



///--- Handlers

function exists(req, res, next) {
  var bucket = req.riak.bucket;
  var key = req.riak.key;

  common.exists(req, bucket, key, function(err, exists) {
    if (err)
      return next(err);

    if (exists)
      return next(new ldap.EntryAlreadyExistsError(key));

    return next();
  });
}



///--- API

module.exports = {

  chain: function(handlers) {
    assert.ok(handlers);

    [
      exists,
      common.parentExists,
      common.operationalAttributes,
      common.buildIndexKeys,
      common.indexesExist,
      common.save,
      common.index,
      common.done
    ].forEach(function(h) {
       handlers.push(h);
     });

    return handlers;
  }

};
