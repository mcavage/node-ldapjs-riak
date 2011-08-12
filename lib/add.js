// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');

var ldap = require('ldapjs');

var common = require('./common');



///--- Globals

var operationsError = common.operationsError;
var ISODateString = common.ISODateString;



///--- Internal Helpers

function exists(req, res, next) {
  common.exists(req, req.riak.bucket, req.riak.key, function(err, exists) {
    if (err)
      return next(err);

    if (exists)
      return next(new ldap.EntryAlreadyExistsError(req.dn.toString()));

    return next();
  });
}


function parentExists(req, res, next) {
  var log = req.riak.log;

  if (req.dn.equals(req.suffix)) {
    log.debug('%s adding suffix (%s)', req.logId, req.suffix.toString());
    return next();
  }

  var parent = req.dn.parent();
  if (!parent) {
    log.error('%s is not suffix(%s), but has no parent',
              req.dn.toString(), req.suffix.toString());
    return next(operationsError());
  }
  var parentKey = encodeURIComponent(parent.toString());

  common.exists(req, req.riak.bucket, parentKey, function(err, exists) {
    if (err)
      return next(err);

    if (!exists)
      return next(new ldap.NoSuchObjectError(parent.toString()));

    return next();
  });
}



function save(req, res, next) {
  var log = req.riak.log;

  var entry = req.toObject();

  req.riak.entry = entry;
  common.save(req, req.riak.bucket, req.riak.key, entry, function(err) {
    if (err)
      return next(err);

    req.riak.entry = entry;
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
    log.debug('%s add of %s done', req.logId, req.dn.toString());

  res.end();
  return next();
}



///--- API

module.exports = {

  chain: function(handlers) {
    assert.ok(handlers);

    [
      exists,
      parentExists,
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
