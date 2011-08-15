// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');

var ldap = require('ldapjs');

var common = require('./common');



///--- Globals

var operationsError = common.operationsError;



///--- Internal Helpers

function load(req, res, next) {
  common.load(req, req.riak.bucket, req.riak.key, function(err, obj) {
    if (err)
      return next(err);

    if (!obj)
      return next(new ldap.NoSuchObjectError(req.dn.toString()));

    return next();
  });
}


function childExists(req, res, next) {
  common.numChildren(req, req.riak.bucket, req.riak.key, function(err, count) {
    if (err)
      return next(err);

    if (count)
      return next(new ldap.NotAllowedOnNonLeafError(req.dn.toString() +
                                                    ' has ' + count +
                                                    ' child entries'));

    return next();
  });
}


function parentExists(req, res, next) {
  if (!req.newSuperior)
    return next();

  var key = encodeURIComponent(req.newSuperior.toString());
  common.exists(req, req.riak.bucket, key, function(err, exists) {
    if (err)
      return next(err);

    if (!exists)
      return next(new ldap.NoSuchObjectError(req.newSuperior.toString()));

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
  req.riak.newDN = newDN;

  if (log.isDebugEnabled())
    log.debug('%s renaming %s to %s',
              req.logId, req.riak.entry.dn.toString(), newDN.toString());

  return next();
}


function del(req, res, next) {
  if (!req.deleteOldRdn)
    return next();

  common.remove(req, req.riak.bucket, req.riak.key, function(err) {
    if (err)
      return next(err);

    return next();
  });
}


function unindexUnique(req, res, next) {
  if (!req.deleteOldRdn)
    return next();

  common.unindexUnique(req, req.riak.entry.attributes, function(err) {
    return next();
  });
}


function unindex(req, res, next) {
  if (!req.deleteOldRdn)
    return next();

  common.unindex(req, req.riak.entry.attributes, function(err) {
    return next();
  });
}


function save(req, res, next) {
  assert.ok(req.riak.entry);

  // Go ahead and overwrite the key/dn, since it's all saves from here out
  var key = encodeURIComponent(req.riak.newDN.toString());
  req.riak.entry.dn = req.riak.newDN.toString();
  req.riak.key = key;

  var bucket = req.riak.bucket;
  var entry = req.riak.entry;

  common.save(req, bucket, key, entry, function(err) {
    if (err)
      return next(err);

    return next();
  });
}


function indexUnique(req, res, next) {
  assert.ok(req.riak.entry);

  common.indexUnique(req, req.riak.entry.attributes, function(err) {
    return next(err);
  });
}


function index(req, res, next) {
  assert.ok(req.riak.entry);

  common.index(req, req.riak.entry.attributes, function(err) {
    return next(err);
  });
}


function done(req, res, next) {
  var log = req.riak.log;

  if (log.isDebugEnabled())
    log.debug('%s modify of %s done', req.logId, req.dn.toString());

  res.end();
  return next();
}


///--- Exported API

module.exports = {

  chain: function(handlers) {
    assert.ok(handlers);

    [
      load,
      childExists,
      parentExists,
      modifyDN,
      del,
      unindexUnique,
      unindex,
      save,
      indexUnique,
      index,
      done
    ].forEach(function(h) {
      handlers.push(h);
    });

    return handlers;
  }

};
