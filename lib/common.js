// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');

var ldap = require('ldapjs');
var retry = require('retry');



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
  return new ldap.OperationsError('riak failure: ' + (err ? err.message : ''));
}


function exists(req, bucket, key, callback) {
  assert.ok(req);
  assert.ok(bucket);
  assert.ok(key);
  assert.ok(callback);

  var log = req.riak.log;
  var db = req.riak.db;

  if (log.isDebugEnabled())
    log.debug('%s %s/%s exists?', req.logId, bucket,
              decodeURIComponent(key));

  var operation = retry.operation(req.riak.retryConfig);
  operation.attempt(function(currentAttempt) {
    return db.head(bucket, key, function(err, obj, meta) {
      if (err && err.statusCode !== 404) {
        if (operation.retry(err)) {
          return;
        } else {
          return next(operationsError(operation.mainError()));
        }
      }

      var exists = (meta && meta.statusCode !== 404);
      if (log.isDebugEnabled())
        log.debug('%s %s/%s exists? %s', req.logId, bucket,
                  decodeURIComponent(key), exists);

      return callback(null, exists, meta);
    });
  });
}


function numChildren(req, bucket, key, callback) {
  assert.ok(req);
  assert.ok(bucket);
  assert.ok(key);
  assert.ok(callback);

  var log = req.riak.log;

  var job = req.riak.db.add({
    bucket: bucket,
    key_filters: [['ends_with', encodeURIComponent(', ') + key]]
  }).map({
    language: 'erlang',
    module: 'riak_kv_mapreduce',
    'function': 'map_object_value'
  });

  if (log.isDebugEnabled())
    log.debug('%s checking %s for child entries: %s', req.logId,
              decodeURIComponent(key), util.inspect(job, false, 10));

  job.run(function(err, data) {
    if (err)
      return callback(operationsError(err));

    return callback(null, data ? data.length : 0);
  });
}


function load(req, bucket, key, callback) {
  assert.ok(req);
  assert.ok(bucket);
  assert.ok(key);
  assert.ok(callback);

  var db = req.riak.db;
  var log = req.riak.log;

  if (log.isDebugEnabled())
    log.debug('%s loading %s/%s', req.logId, bucket, decodeURIComponent(key));

  var operation = retry.operation(req.riak.retryConfig);
  operation.attempt(function(currentAttempt) {
    return db.get(bucket, key, function(err, obj, meta) {
      if (err && err.statusCode !== 404) {
        if (operation.retry(err))
          return;

        return callback(operationsError(operation.mainError()));
      }

      if (obj && obj.attributes)
        req.riak.entry = obj;

      if (log.isDebugEnabled())
        log.debug('%s loaded %s/%s -> %j', req.logId, bucket,
                  decodeURIComponent(key), req.riak.entry || {});

      req.riak.meta = meta;
      return callback(null, req.riak.entry || null);
    });
  });
}


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


///--- Exported API

module.exports = {

  exists: exists,
  load: load,
  remove: remove,
  save: save,
  numChildren: numChildren,
  indexUnique: indexUnique,
  index: index,
  unindexUnique: unindexUnique,
  unindex: unindex,

  operationsError: operationsError,
  ISODateString: ISODateString

};
