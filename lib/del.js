// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');

var ldap = require('ldapjs');

var common = require('./common');



///--- Globals

var operationsError = common.operationsError;



///--- Handlers

function del(req, res, next) {
  var bucket = req.riak.bucket;
  var client = req.riak.client;
  var key = req.riak.key;
  var log = req.riak.log;

  log.debug('%s removing %s', req.logId, key);
  return client.del(bucket, key, function(err) {
    if (err)
      return next(operationsError(err));

    res.psentry = {
      dn: req.dn.toString(),
      attributes: {
        objectclass: '*'
      }
    };
    log.debug('%s removed %s', req.logId, key);
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
      changetype: 'delete'
    }
  };
  var opts = {
    indexes: req.riak.changelog.indexes
  };
  var obj = req.riak.entry;
  if (obj.attributes.userpassword)
    obj.attributes.userpassword = 'XXXXXX';
  entry.attributes.changes = JSON.stringify(obj.attributes);
  entry.attributes.objectclass = 'changeLogEntry';

  // tack changelog entry to the response object
  res.changelog = entry;

  return client.put(bucket, key.dn, entry, opts, function(err, obj) {
    if (err)
      return next(operationsError(err));

    log.debug('%s changelogging %s', req.logId, req.dn.toString());
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
      changelog,
      del,
      common.buildIndexKeys,
      common.unindex,
      common.done,
      common.updatePersistentSearchClients
    ].forEach(function(h) {
       handlers.push(h);
     });

    return handlers;
  }

};
