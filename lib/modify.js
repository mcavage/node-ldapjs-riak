// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');

var ldap = require('ldapjs');

var common = require('./common');



///--- Handlers

function modify(req, res, next) {
  assert.ok(req.riak.entry);
  var log = req.riak.log;

  var attributes = req.riak.entry.attributes;
  var delta = {};

  // Modify the loaded entry
  log.debug('%s processing modifications %j', req.logId, req.changes);
  req.changes.forEach(function(change) {
    var mod = change.modification;

    switch (change.operation) {

    case 'add':
      if (!attributes[mod.type])
        attributes[mod.type] = [];
      mod.vals.forEach(function(v) {
        if (attributes[mod.type].indexOf(v) === -1) {
          attributes[mod.type].push(v);
          if (!delta[mod.type])
            delta[mod.type] = [];
          delta[mod.type].push(v);
        }
      });
      break;

    case 'delete':
      if (!attributes[mod.type])
        return; // Just silently allow this.

      if (!mod.vals || !mod.vals.length) {
        delete attributes[mod.type];
      } else {
        mod.vals.forEach(function(v) {
          var index = attributes[mod.type].indexOf(v);
          if (index !== -1)
            attributes[mod.type].splice(index, 1);
        });

        if (attributes[mod.type].length === 0)
          delete attributes[mod.type];
      }
      break;

    case 'replace':
      if (!attributes[mod.type])
        attributes[mod.type] = [];
      if (!delta[mod.type])
        delta[mod.type] = [];

      if (!mod.vals || !mod.vals.length) {
        delete attributes[mod.type];
      } else {
        // If there are unique indexes present, don't do an
        // update if the values are actually the same (delta only)
        var diff = true;
        if (attributes[mod.type].length === mod.vals.length) {
          attributes[mod.type].sort();
          mod.vals.sort();
          for (var i = 0; i < mod.vals.length; i++) {
            diff &= (attributes[mod.type][i] !== mod.vals[i]);
            if (!diff)
              break;
          }
        }
        attributes[mod.type] = mod.vals.slice();
        if (diff)
          delta[mod.type] = mod.vals.slice();
      }
      break;
    }
  });

  log.debug('%s using delta %j to check unique indexes next', req.logId, delta);
  req.riak.entry.attributes = delta;
  req.riak.stashedAttributes = attributes;
  return next();
}


function stash(req, res, next) {
  assert.ok(req.riak.uniqueIndexKeys);

  var log = req.riak.log;

  log.debug('%s stashing indexes: %j', req.logId, req.riak.uniqueIndexKeys);
  req.riak.stashedIndexKeys = req.riak.uniqueIndexKeys;
  req.riak.uniqueIndexKeys = [];
  return next();
}

function pop(req, res, next) {
  assert.ok(req.riak.stashedIndexKeys);
  assert.ok(req.riak.stashedAttributes);
  var log = req.riak.log;

  req.riak.uniqueIndexKeys = req.riak.stashedIndexKeys;
  req.riak.stashedIndexKeys = null;
  req.riak.entry.attributes = req.riak.stashedAttributes;
  req.riak.stashedAttributes = null;

  log.debug('%s restored indexes: %j and entry %j',
            req.logId, req.riak.uniqueIndexKeys, req.riak.entry);
  return next();
}


///--- Exported API

module.exports = {

  chain: function(handlers) {
    assert.ok(handlers);

    [
      common.load,
      common.buildIndexKeys,
      stash,
      modify,
      common.buildIndexKeys,
      common.indexesExist,
      pop,
      common.unindex,
      common.operationalAttributes,
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
