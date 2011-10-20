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


function changelog(req, res, next) {
  if (!req.riak.changelogBucket)
    return next();

  var bucket = req.riak.changelogBucket;
  var client = req.riak.client;
  var log = req.riak.log;

  log.debug('%s changelogging %s', req.logId, req.dn.toString());

  var key = req.riak.changelog.nextChangeNumber;
  var now = new Date();
  var entry = {
    dn: key.dn,
    attributes: {
      targetdn: req.dn.toString(),
      changetime: common.ISODateString(),
      changenumber: key.changeNumber,
      changetype: 'add'
    }
  };
  var opts = {
    indexes: req.riak.changelog.indexes
  };
  var obj = req.toObject();
  if (obj.attributes.userpassword)
    obj.attributes.userpassword = 'XXXXXX';
  entry.attributes.changes = JSON.stringify(obj.attributes);
  entry.attributes.objectclass = 'changeLogEntry';

  return client.put(bucket, key.dn, entry, opts, function(err, obj) {
    if (err)
      return next(operationsError(err));

    log.debug('%s changelogging %s', req.logId, req.dn.toString());
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
      common.done,
      changelog
    ].forEach(function(h) {
       handlers.push(h);
     });

    return handlers;
  }

};
