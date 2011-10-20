// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');

var ldap = require('ldapjs');

var common = require('./common');



///--- Globals

var operationsError = common.operationsError;



///--- Handlers



function newParentExists(req, res, next) {
  if (!req.newSuperior)
    return next();

  var bucket = req.riak.bucket;
  var key = req.newSuperior.toString();
  return common.exists(req, bucket, key, function(err, exists) {
    if (err)
      return next(err);

    if (!exists)
      return next(new ldap.NoSuchObjectError(key));

    return next();
  });
}


function del(req, res, next) {
  if (!req.deleteOldRdn)
    return next(new ldap.UnwillingtoPerformError('must delete old rdn'));

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


function modifyDN(req, res, next) {
  var log = req.riak.log;

  var newDN;
  if (req.newSuperior) {
    newDN = req.newSuperior;
  } else {
    newDN = ldap.parseDN(req.riak.entry.dn);
    newDN.rdns.shift();
  }
  newDN.rdns.unshift(req.newRdn.rdns[0]);
  req.riak.entry.dn = newDN.toString();
  req.riak.key = req.riak.newDN;

  log.debug('%s renaming %s to %s', req.logId, req.riak.key, newDN.toString());
  return next();
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
      changetype: 'modrdn',
      newrdn: req.newRdn.toString(),
      deteleoldrdn: req.deleteOldRdn.toString().toUpperCase()
    }
  };
  if (req.newSuperior)
    entry.attributes.newsuperior = req.newSuperior.toString();

  entry.attributes.objectclass = 'changeLogEntry';

  var opts = {
    indexes: req.riak.changelog.indexes
  };
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
      newParentExists,
      del,
      common.buildIndexKeys,
      common.unindex,
      modifyDN,
      common.buildIndexKeys,
      common.save,
      common.index,
      common.done
    ].forEach(function(h) {
      handlers.push(h);
    });

    return handlers;
  }

};
