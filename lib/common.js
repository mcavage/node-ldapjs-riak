// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');

var ldap = require('ldapjs');



///--- Globals

var parseDN = ldap.parseDN;

var EntryChangeNotificationControl = ldap.EntryChangeNotificationControl;
var PersistentSearch = require('./persistent_search');

var PS = new PersistentSearch();

var CHANGELOG_DN = parseDN('cn=changelog');

///--- API

function ISODateString() {
  function pad(n) {
    return n < 10 ? '0' + n : n;
  }

  var d = new Date();

  return d.getUTCFullYear() + '-' +
    pad(d.getUTCMonth() + 1) + '-' +
    pad(d.getUTCDate()) + 'T' +
    pad(d.getUTCHours()) + ':' +
    pad(d.getUTCMinutes()) + ':' +
    pad(d.getUTCSeconds()) + 'Z';
}


function operationsError(err) {
  var msg = err && err.message ? err.message : '';
  return new ldap.OperationsError('riak failure: ' + msg,
                                  null, operationsError);
}


function exists(req, bucket, key, callback) {
  assert.ok(req);
  assert.ok(bucket);
  assert.ok(key);
  assert.ok(callback);

  var log = req.riak.log;

  log.debug('%s exists(%s/%s) entered', req.logId, bucket, key);
  return req.riak.client.head(bucket, key, function(err, obj, headers) {
    var exists = true;
    if (err) {
      if (err.code !== 404)
        return callback(operationsError(err));

      exists = false;
    }

    log.debug('%s exists(%s/%s) %s', req.logId, bucket, key, exists);
    return callback(null, exists);
  });
}


function childExists(req, res, next) {
  var bucket = req.riak.bucket;
  var client = req.riak.client;
  var key = req.riak.key;
  var log = req.riak.log;

  log.debug('%s looking for children of %s', req.logId, key);
  return client.listKeys(bucket, function(err, keys) {
    if (err)
      return next(operationsError(err));

    if (!keys || !keys.length)
      return next();

    for (var i = 0; i < keys.length; i++)
      if (req.dn.parentOf(keys[i]))
        return next(new ldap.NotAllowedOnNonLeafError(keys[i]));

    log.debug('%s %s has no children', req.logId, key);
    return next();
  });
}


function parentExists(req, res, next) {
  var bucket = req.riak.bucket;
  var log = req.riak.log;

  if (req.dn.equals(req.suffix)) {
    log.debug('%s adding suffix (%s)', req.logId, req.riak.key);
    return next();
  }

  var parent = req.dn.parent();
  assert.ok(parent);

  var key = parent.toString();
  return exists(req, bucket, key, function(err, success) {
    if (err)
      return next(err);

    if (!success)
      return next(new ldap.NoSuchObjectError(key));

    return next();
  });
}


function load(req, res, next) {
  var bucket = req.riak.bucket;
  var client = req.riak.client;
  var key = req.riak.key;
  var log = req.riak.log;

  log.debug('%s loading %s', req.logId, bucket, key);
  return client.get(bucket, key, function(err, obj) {
    if (err) {
      if (err.code !== 404)
        return next(operationsError(err));

      return next(new ldap.NoSuchObjectError(key));
    }

    log.debug('%s loaded %s -> %j', req.logId, bucket, key, obj);
    req.riak.entry = obj;
    return next();
  });
}


function buildIndexKeys(req, res, next) {
  assert.ok(req.riak.entry);

  req.riak.uniqueIndexKeys = [];

  if (!req.riak.uniqueIndexes || !req.riak.uniqueIndexes.length)
    return next();

  var entry = req.riak.entry.attributes;
  req.riak.uniqueIndexes.forEach(function(i) {
    if (!entry.hasOwnProperty(i))
      return;

    entry[i].forEach(function(v) {
      var key = i + ': ' + v;
      if (req.riak.uniqueIndexKeys.indexOf(key) === -1)
        req.riak.uniqueIndexKeys.push(key);
    });
  });

  return next();
}


function indexesExist(req, res, next) {
  assert.ok(req.riak.entry);
  assert.ok(req.riak.uniqueIndexKeys);

  var log = req.riak.log;

  if (!req.riak.uniqueIndexKeys.length) {
    log.debug('%s indexesExist(%s) no-op', req.logId, req.riak.key);
    return next();
  }

  log.debug('%s indexesExist(%s) entered %j',
            req.logId, req.riak.key, req.riak.uniqueIndexKeys);

  var done = false;
  var bucket = req.riak.uniqueIndexBucket;
  var entry = req.riak.entry.attributes;

  var finished = 0;
  return req.riak.uniqueIndexKeys.forEach(function(k) {
    exists(req, bucket, k, function(err, exists) {
      if (err && !done) {
        done = true;
        return next(err);
        }

      if (exists && !done) {
        done = true;
        return next(new ldap.ConstraintViolationError(k));
      }

      if (++finished >= req.riak.uniqueIndexKeys.length && !done) {
        done = true;
        log.debug('%s uniqueIndexesExist(%s) ok', req.logId, req.riak.key);
        return next();
      }
    });
  });
}

function index(req, res, next) {
  assert.ok(req.riak.uniqueIndexKeys);

  var bucket = req.riak.uniqueIndexBucket;
  var client = req.riak.client;
  var done = 0;
  var finished = 0;
  var log = req.riak.log;

  if (!req.riak.uniqueIndexKeys.length)
    return next();

  log.debug('%s saving unique indexes for %s: %j',
            req.logId, req.riak.key, req.riak.uniqueIndexKeys);

  req.riak.uniqueIndexKeys.forEach(function(k) {
    var obj = { bucket: req.riak.bucket, key: req.riak.key };
    return client.put(bucket, k, obj, {}, function(err) {
      if (err && !done) {
        done = true;
        return next(operationsError(err));
      }

      if (++finished === req.riak.uniqueIndexKeys.length && !done) {
        done = true;
        log.debug('%s unique indexes for %s saved', req.logId, req.riak.key);
        return next();
      }
    });
  });
}


function unindex(req, res, next) {
  assert.ok(req.riak.entry);
  assert.ok(req.riak.uniqueIndexKeys);

  var bucket = req.riak.uniqueIndexBucket;
  var client = req.riak.client;
  var entry = req.riak.entry.attributes;
  var keys = req.riak.uniqueIndexKeys;
  var log = req.riak.log;

  log.debug('%s indexes to purge: %j', req.logId, keys);
  if (!keys.length)
    return next();

  var done = false;
  var finished = 0;
  keys.forEach(function(k) {
    log.debug('%s deleting index %s', req.logId, k);
    return client.del(bucket, k, function(err) {
      if (err && err.code !== 404 && !done) {
        done = true;
        return next(operationsError(err));
      }

      if (err && err.code === 404) {
        log.warn('%s unable to purge unique index /riak/%s/%s',
                 req.logId, bucket, k);
      }

      log.debug('%s deleted index %s', req.logId, k);
      if ((++finished === keys.length) && !done) {
        done = true;
        return next();
      }
    });
  });
}


function save(req, res, next) {
  assert.ok(req.riak.entry);

  var bucket = req.riak.bucket;
  var client = req.riak.client;
  var entry = req.riak.entry;
  var key = req.riak.key;
  var log = req.riak.log;

  if (entry.attributes.objectclass) {
    for (var i = 0; i < entry.attributes.objectclass.length; i++)
      entry.attributes.objectclass[i] =
      entry.attributes.objectclass[i].toLowerCase();
  }


  log.debug('%s saving %s: %j', req.logId, key, entry);
  // cache the entry
  res.psentry = entry;

  var opts = {
    indexes: req.riak.indexes
  };
  return client.put(bucket, key, entry, opts, function(err, obj) {
    if (err)
      return next(operationsError(err));

    log.debug('%s %s saved', req.logId, key);
    return next();
  });
}


function done(req, res, next) {
  var log = req.riak.log;
  log.debug('%s key=%s done', req.logId, req.riak.key);
  if (req.persistentSearch) {
    // do not close the connection and register the req and res
    PS.addClient(req, res);
    res.connection.addListener('end', function() {
      // deregister the connection
      PS.removeClient(req, res);
    });
  } else {
    res.end();
    return next();
  }
}


function operationalAttributes(req, res, next) {
  var log = req.riak.log;
  log.debug('%s operationAttributes(%s) entered', req.logId, req.riak.key);

  if (!req.riak.entry)
    req.riak.entry = req.toObject();

  var attributes = req.riak.entry.attributes;
  if (!attributes._ctime)
    attributes._ctime = [ISODateString()];
  if (!attributes._createdfrom)
    attributes._createdfrom = [req.logId];
  if (!attributes._createdby)
    attributes._createdby = [req.connection.ldap.bindDN.toString()];
  attributes._mtime = [ISODateString()];
  attributes._modifiedfrom = [req.logId];
  attributes._modifiedby = [req.connection.ldap.bindDN.toString()];

  log.debug('%s operationalAttributes(%s) -> %j',
            req.logId, req.riak.key, req.riak.entry);
  return next();
}


function updatePersistentSearchClients(req, res, next) {
  // notify all pertinent clients of change
  // also check that the request.dn is for the changelog,
  // if so, handle differently
  PS.clientList.forEach(function(element, index, array) {
    // see if the change type of the PS request is the same as the current req
    if (PersistentSearch.checkChangeType(element.req, req.type)) {
      var changelog = res.changelog;
      var control = PersistentSearch.
        getEntryChangeNotificationControl(element.req, changelog);
      var entry = res.psentry;

      // if the cached PS request was for the changelog
      if (element.req.dn.equals(CHANGELOG_DN))
        entry = changelog;

      sendSearchRequest(element.req, element.res, entry,
                        entry.dn, control, next);
    }
  });

  return next();
}


function sendSearchRequest(req, res, obj, dn, controls, callback) {
  var log = req.riak.log;
  var attrs = obj.attributes;

  var send = false;

  switch (req.scope) {
  case 'base':
    if (req.dn.equals(dn) && req.filter.matches(attrs))
      send = true;
    break;
  case 'one':
    if ((req.dn.parentOf(dn) || req.dn.equals(dn)) &&
        ((dn.rdns.length - req.dn.rdns.length) <= 1) &&
        req.filter.matches(attrs))
      send = true;
    break;
  case 'sub':
    if ((req.dn.parentOf(dn) || req.dn.equals(dn)) &&
        req.filter.matches(attrs))
      send = true;
    break;
  }

  if (send) {
    if (controls) {
      // deep copy the obj so we can tack on the control, since the obj maybe
      // used by another request
      obj = JSON.parse(JSON.stringify(obj));
      if (controls.isArray) {
        obj.controls = controls;
      } else {
        obj.controls = [];
        obj.controls.push(controls);
      }
    }

    log.debug('%s sending: %j', req.logId, obj);
    res.send(obj, req.hidden);
  }

  return callback();
}


///--- Exports

module.exports = {
  ISODateString: ISODateString,
  operationsError: operationsError,
  exists: exists,
  childExists: childExists,
  parentExists: parentExists,
  load: load,
  buildIndexKeys: buildIndexKeys,
  indexesExist: indexesExist,
  index: index,
  unindex: unindex,
  save: save,
  done: done,
  operationalAttributes: operationalAttributes,
  updatePersistentSearchClients: updatePersistentSearchClients,
  sendSearchRequest: sendSearchRequest
};


