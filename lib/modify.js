// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');
var util = require('util');

var ldap = require('ldapjs');

var common = require('./common');



///--- Handlers

function modify(req, res, next) {
  assert.ok(req.riak.entry);
  var log = req.riak.log;

  var attributes = req.riak.entry.attributes;
  var delta = {};

  // Modify the loaded entry
  if (log.isDebugEnabled()) {
    var msg = '';
    req.changes.forEach(function(c) {
      msg += JSON.stringify(c.json);
    });
    log.debug('%s processing modifications %s', req.logId, msg);
  }
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
      changetype: 'modify'
    }
  };
  var changes = [];
  req.changes.forEach(function(c) {
    if (c.modification.type.toLowerCase() === 'userpassword')
      c.modification.vals = ['XXXXXX'];
    changes.push(c.json);
  });

  entry.attributes.changes = JSON.stringify(changes);
  entry.attributes.objectclass = 'changeLogEntry';

  var opts = {
    indexes: req.riak.changelog.indexes
  };

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
      common.buildIndexKeys,
      stash,
      modify,
      common.buildIndexKeys,
      common.indexesExist,
      pop,
      common.unindex,
      common.operationalAttributes,
      common.buildIndexKeys,
      changelog,
      common.save,
      common.index,
      common.done,
      common.updatePersistentSearchClients
    ].forEach(function(h) {
      handlers.push(h);
    });

    return handlers;
  }

};
