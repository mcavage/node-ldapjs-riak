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


function modify(req, res, next) {
  assert.ok(req.riak.entry);

  var object = req.riak.entry.attributes;

  // Modify the loaded entry
  req.changes.forEach(function(change) {
    var mod = change.modification;

    switch (change.operation) {

    case 'add':
      if (!object[mod.type])
        object[mod.type] = [];
      mod.vals.forEach(function(v) {
        if (object[mod.type].indexOf(v) === -1)
          object[mod.type].push(v);
      });
      break;

    case 'delete':
      if (!object[mod.type])
        return next(); // Just silently allow this.

      if (!mod.vals || !mod.vals.length) {
        delete object[mod.type];
      } else {
        mod.vals.forEach(function(v) {
          var index = object[mod.type].indexOf(v);
          if (index !== -1)
            object[mod.type].splice(index, 1);
        });
      }
      break;

    case 'replace':
      if (!object[mod.type])
        object[mod.type] = [];
      if (!mod.vals || !mod.vals.length) {
        delete object[mod.type];
      } else {
        object[mod.type] = mod.vals.splice(0);
      }
      break;
    }
  });

  req.riak.entry.attributes = object;
  return next();
}



function save(req, res, next) {
  assert.ok(req.riak.entry);

  var bucket = req.riak.bucket;
  var entry = req.riak.entry;
  var key = req.riak.key;

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
      unindexUnique,
      unindex,
      modify,
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
