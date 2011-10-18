// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');

var ldap = require('ldapjs');



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
  return new ldap.OperationsError('riak failure: ' + msg);
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
        return next(operationsError(err));

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
    var links = [{ bucket: req.riak.bucket, key: req.riak.key }];
    return client.link(bucket, k, links, function(err) {
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
  res.end();
  return next();
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
  operationalAttributes: operationalAttributes
};


/*
function save(req, bucket, key, entry, meta, callback) {
  assert.ok(req);
  assert.ok(bucket);
  assert.ok(key);
  assert.ok(entry);
  assert.ok(meta);

  if (typeof(meta) === 'function') {
    callback = meta;
    meta = null;
  }

  var db = req.riak.db;
  var log = req.riak.log;

  if (!entry.attributes)
    entry.attributes = {};

  if (!entry.attributes._ctime) {
    entry.attributes._ctime = [ISODateString()];
    entry.attributes._created_from = [req.logId];
    entry.attributes._created_by = [req.connection.ldap.bindDN.toString()];
  }
  entry.attributes._mtime = [ISODateString()];
  entry.attributes._modified_from = [req.logId];
  entry.attributes._modified_by = [req.connection.ldap.bindDN.toString()];

  if (log.isDebugEnabled())
    log.debug('%s saving %s/%s: entry=%j, meta=%j', req.logId, bucket,
              decodeURIComponent(key), entry, meta || {});

  function _callback(err, obj, meta) {
    if (err) {
      if (operation.retry(err)) {
        return;
      } else {
        return callback(operationsError(operation.mainError()));
      }
    }

    if (log.isDebugEnabled())
      log.debug('%s saved %s/%s', req.logId, bucket,
                decodeURIComponent(key));

    return callback(null);
  }


  var operation = retry.operation(req.riak.retryConfig);
  operation.attempt(function(currentAttempt) {
    db.save(bucket,
            key,
            entry,
            (meta || _callback),
            (meta ? _callback : undefined));
  });
}


function remove(req, bucket, key, callback) {
  assert.ok(req);
  assert.ok(bucket);
  assert.ok(key);
  assert.ok(callback);

  var db = req.riak.db;
  var log = req.riak.log;

  if (log.isDebugEnabled())
    log.debug('%s deleting: %s/%s', req.logId, bucket, decodeURIComponent(key));

  var operation = retry.operation(req.riak.retryConfig);
  operation.attempt(function(currentAttempt) {
    return db.remove(bucket, key, function(err, obj, meta) {
      if (err) {
        if (operation.retry(err))
          return;

        return callback(operationsError(operation.mainError()));
      }

      if (log.isDebugEnabled())
        log.debug('%s deleted %s', req.logId, decodeURIComponent(key));

      return callback();
    });
  });
}

function addIndexToMeta(req, bucket, key, meta) {
  assert.ok(req);
  assert.ok(bucket);
  assert.ok(key);

  if (!meta)
    meta = {links: []};

  // If the link already exists, this is a no-op
  for (var i = 0; i < meta.links.length; i++) {
    var link = meta.links[i];
    if (link.bucket === bucket && link.key === key)
      return null;
  }

  meta.links.push({
    bucket: bucket,
    key: key,
    tag: req.dn.toString()
  });

  return meta;
}


function _index(req, object, indexes, getBucket, getKey, unique, callback) {
  assert.ok(req);
  assert.ok(object);
  assert.ok(indexes);
  assert.ok(getBucket);
  assert.ok(getKey);
  assert.ok(callback);

  var log = req.riak.log;

  var keys = Object.keys(object);

  var attributes = [];
  indexes.forEach(function(index) {
    if (keys.indexOf(index) !== -1)
      attributes.push(index);
  });

  if (!attributes.length) {
    log.debug('%s no indexes for %s', req.logId, req.dn.toString());
    return callback();
  }

  var finished = 0;
  attributes.forEach(function(index) {
    var valsFinished = 0;
    object[index].forEach(function(v) {
      var bucket = getBucket(index);
      var key = getKey(v);
      exists(req, bucket, key, function(err, exists, meta) {
        if (err) {
          log.error('%s error indexing %s (MANUAL RECOVERY NEEDED): %s',
                    req.logId, bucket, req.dn.toString(), err.stack);
          return callback();
        }

        if (exists && unique)
          return callback(new ldap.ConstraintViolationError(index));

        meta = addIndexToMeta(req,
                              req.riak.bucket,
                              req.riak.key,
                              exists ? meta : null);

        if (!meta)
          return callback();

        save(req, bucket, key, {}, meta, function(err) {
          if (err) {
            log.error('%s error indexing %s/%s (MANUAL RECOVERY NEEDED): %s',
                      req.logId, req.riak.bucket, req.dn.toString(), err.stack);
            return callback();
          }

          if (++valsFinished === object[index].length)
            if (++finished === attributes.length)
              return callback();
        });
      });
    });
  });
}


function indexUnique(req, object, callback) {
  assert.ok(req);
  assert.ok(object);
  assert.ok(callback);

  function getBucket(index) {
    return req.riak.uniqueIndexesBucket + '_' + index;
  }

  function getKey(val) {
    return encodeURIComponent(val);
  }

  return _index(req,
                object,
                req.riak.uniqueIndexes,
                getBucket,
                getKey,
                true,
                callback);
}


function index(req, object, callback) {
  assert.ok(req);
  assert.ok(object);
  assert.ok(callback);

  function getBucket(index) {
    return req.riak.indexesBucket + '_' + index;
  }

  function getKey(val) {
    return encodeURIComponent(val) + '_' + req.riak.key;
  }

  return _index(req,
                object,
                req.riak.indexes,
                getBucket,
                getKey,
                false,
                callback);
}


function _unindex(req, object, indexes, getBucket, getKey, callback) {
  assert.ok(req);
  assert.ok(object);
  assert.ok(indexes);
  assert.ok(getBucket);
  assert.ok(getKey);
  assert.ok(callback);

  var log = req.riak.log;

  var keys = Object.keys(object);

  var attributes = [];
  indexes.forEach(function(index) {
    if (keys.indexOf(index) !== -1)
      attributes.push(index);
  });

  if (!attributes.length)
    return callback();

  var finished = 0;
  indexes.forEach(function(index) {
    var bucket = getBucket(index);

    var valsFinished = 0;
    object[index].forEach(function(v) {
      var key = getKey(v);

      remove(req, bucket, key, function(err) {
        if (err) {
          log.error('%s error unindexing %s (MANUAL RECOVERY NEEDED): %s',
                    req.logId, bucket, decodeURIComponent(key), err.stack);
        }

        if (++valsFinished === object[index].length)
          if (++finished === indexes.length)
            return callback();
      });
    });
  });
}


function unindexUnique(req, object, callback) {
  assert.ok(req);
  assert.ok(object);
  assert.ok(callback);

  function getBucket(index) {
    return req.riak.uniqueIndexesBucket + '_' + index;
  }

  function getKey(val) {
    return encodeURIComponent(val);
  }

  return _unindex(req,
                  object,
                  req.riak.uniqueIndexes,
                  getBucket,
                  getKey,
                  callback);
}


function unindex(req, object, callback) {
  assert.ok(req);
  assert.ok(object);
  assert.ok(callback);

  function getBucket(index) {
    return req.riak.indexesBucket + '_' + index;
  }

  function getKey(val) {
    return encodeURIComponent(val) + '_' + req.riak.key;
  }

  return _unindex(req,
                  object,
                  req.riak.indexes,
                  getBucket,
                  getKey,
                  callback);
}
*/
