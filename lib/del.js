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
  var log = req.riak.log;

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


function del(req, res, next) {
  common.remove(req, req.riak.bucket, req.riak.key, function(err) {
    if (err)
      return next(err);

    return next();
  });
}


function unindexUnique(req, res, next) {
  assert.ok(req.riak.entry);

  common.unindexUnique(req, req.riak.entry.attributes, function(err) {
    return next();
  });
}


function unindex(req, res, next) {
  assert.ok(req.riak.entry);

  common.unindex(req, req.riak.entry.attributes, function(err) {
    return next();
  });
}



function done(req, res, next) {
  var log = req.riak.log;
  if (log.isDebugEnabled())
    log.debug('%s delete of %s successful', req.logId, req.dn.toString());

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
      del,
      unindexUnique,
      unindex,
      done
    ].forEach(function(h) {
       handlers.push(h);
     });

    return handlers;
  }

};
