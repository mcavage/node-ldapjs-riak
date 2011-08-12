// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');
var util = require('util');

var ldap = require('ldapjs');

var common = require('./common');



///--- Globals

var operationsError = common.operationsError;
var parseDN = ldap.parseDN;


///--- Internal Functions

function isIndexedUnique(req, attr) {
  assert.ok(req);
  assert.ok(attr);

  for (var i = 0; i < req.riak.uniqueIndexes.length; i++)
    if (req.riak.uniqueIndexes[i] === attr)
      return req.riak.uniqueIndexesBucket;

  return false;
}


function isIndexed(req, attr) {
  assert.ok(req);
  assert.ok(attr);

  for (var i = 0; i < req.riak.indexes.length; i++)
    if (req.riak.indexes[i] === attr)
      return req.riak.indexesBucket;

  return false;
}


function filterToJob(req, filter) {
  assert.ok(req);
  if (!filter)
    filter = req.filter;

  var db = req.riak.db;
  var key = req.riak.key;
  var log = req.riak.log;

  var indexBucket;
  var uniqueIndexBucket;
  var job;

  if (req.scope === 'sub' &&
      ((filter.attribute &&
            ((uniqueIndexBucket = isIndexedUnique(req, filter.attribute)) ||
             (indexBucket = isIndexed(req, filter.attribute)))) ||
       filter.type === 'and')) {

    var bucket = (uniqueIndexBucket || indexBucket) +
      '_' + filter.attribute;
    switch (filter.type) {

    case 'equal':
    case 'approx':
      if (uniqueIndexBucket) {
        job = db.add([[
          bucket,
          encodeURIComponent(filter.value)
        ]]);
      } else {
        job = db.add({
          bucket: bucket,
          key_filters: [
            [
              'and',
              [['starts_with', encodeURIComponent(filter.value)]],
              [['ends_with', key]]
            ]
          ]
        });
      }
      break;

    case 'substring':
      if (uniqueIndexBucket) {
        job = db.add({
          bucket: bucket,
          key_filters: [['starts_with', encodeURIComponent(filter.initial)]]
        });
      } else {
        job = db.add({
          bucket: bucket,
          key_filters: [
            [
              'and',
              [['starts_with', encodeURIComponent(filter.initial)]],
              [['ends_with', key]]
            ]
          ]
        });
      }
      break;

    case 'present':
      job = db.add(bucket);
      break;

    case 'and':
      for (var i = 0; i < filter.filters.length; i++)
        if ((job = filterToJob(req, filter.filters[i])))
          return job;

      break;

    case 'ge':
      if (uniqueIndexBucket) {
        job = db.add({
          bucket: bucket,
          key_filters: [
            [['url_decode']],
            [['greater_than_eq', filter.value]]
          ]
        });
      } else {
        job = db.add({
          bucket: bucket,
          key_filters: [
            [['url_decode']],
            [
              'and',
              [['greater_than_eq', filter.value]],
              [['ends_with', req.dn.toString()]]
            ]
          ]
        });
      }
      break;

    case 'le':
      if (uniqueIndexBucket) {
        job = db.add({
          bucket: bucket,
          key_filters: [
            [['url_decode']],
            [['less_than_eq', filter.value]]
          ]
        });
      } else {
        job = db.add({
          bucket: bucket,
          key_filters: [
            [['url_decode']],
            [
              'and',
              [['less_than_eq', filter.value]],
              [['ends_with', req.dn.toString()]]
            ]
          ]
        });
      }
      break;
    }
  }

  if (job) {
    job.link({
      bucket: req.riak.bucket,
      keep: false
    });
  }

  return job;
}


function subtreeSearch(req, res, next) {
  var log = req.riak.log;
  var db = req.riak.db;
  var key = req.riak.key;

  var job = filterToJob(req);

  if (!job) {
    job = db.add({
      bucket: req.riak.bucket,
      key_filters: [['ends_with', req.riak.key]]
    });
  }

  job.map({
    language: 'erlang',
    module: 'riak_kv_mapreduce',
    'function': 'map_object_value'
  }).reduce({
    language: 'erlang',
    module: 'riak_kv_mapreduce',
    'function': 'reduce_set_union'
  });

  if (log.isDebugEnabled())
    log.debug('%s: search generated: %s',
              req.logId, util.inspect(job, false, 10));

  var timeout = req.timeLimit * 1000 || 45000;
  return job.run({
    timeout: timeout
  }, function(err, data) {
    if (err) {
      try {
        if (JSON.parse(err.message.match(/\{.* /)).error === 'timeout') {
          return next(ldap.TimeLimitExceededError(timeout + 's'));
        }
      } catch (e) {}
      log.warn('%s riak failure:  %s', req.logId, err.stack);
      return next(operationsError(e));
    } // end if (err)

    if (log.isTraceEnabled())
      log.trace('%s: riak search returned: %s', req.logId, data.join());

    // This is fairly costly on large result sets, but we'll optimize later.
    var entries = [];
    data.forEach(function(d) {
      var e = JSON.parse(d);
      e.dn = parseDN(e.dn);
      entries.push(e);
    });

    entries.sort(function(a, b) {
      if (a.dn.rdns.length < b.dn.rdns.length) return -1;
      if (a.dn.rdns.length > b.dn.rdns.length) return 1;

      for (var i = a.dn.rdns.length - 1; i >= 0; i--) {
        if (a.dn.rdns[i] < b.dn.rdns[i]) return -1;
        if (a.dn.rdns[i] > b.dn.rdns[i]) return 1;
      }

      return 0;
    });

    if (req.sizeLimit)
      entries = entries.slice(0, req.sizeLimit);

    entries.forEach(function(e) {
      // WTF does this do you ask?  Ok, M/R gives back all the keys in the
      // bucket (potentially), so we have to check that the current entry
      // is (1) either the base object, or under it, (2) if it's a one-level
      // search that the depth is right (or just let it through if it's sub),
      // and (3) that the search filter actually matches.
      if ((req.dn.parentOf(e.dn) || req.dn.equals(e.dn)) &&
          (req.scope === 'sub' ||
           (e.dn.rdns.length - req.dn.rdns.length) <= 1) &&
          (req.filter.matches(e.attributes))) {
        res.send(e);
      } else {
        if (log.isTraceEnabled())
          log.trace('%s skipping entry %s', req.logId, e.dn.toString());
      }
    });

    return next();
  });
}


function baseSearch(req, res, next) {
  var log = req.riak.log;
  var bucket = req.riak.bucket;
  var db = req.riak.db;
  var key = req.riak.key;

  common.load(req, req.riak.bucket, req.riak.key, function(err, obj) {
    if (err)
      return next(err);

    if (!obj)
      return next(new ldap.NoSuchObjectError(req.dn.toString()));

    if (req.filter.matches(obj.attributes)) {
      res.send(obj);
    } else {
      if (log.isDebugEnabled())
        log.debug('%s filter didn\'t match', req.logId);
    }

    return next();
  });
}


function done(req, res, next) {
  res.end();
  return next();
}


function search(req, res, next) {
  var log = req.riak.log;

  if (log.isDebugEnabled())
    log.debug('%s searching %j', req.logId, req.json);

  try {
    switch (req.scope) {
    case 'base':
      return baseSearch(req, res, next);
    case 'one':
    case 'sub':
      return subtreeSearch(req, res, next);
    }
  } catch (e) {
    log.warn('%s invalid search scope: %s', req.logId, e.stack);
    return next(new ldap.ProtocolError(e.message));
  }
}



///--- API

module.exports = {

  chain: function(handlers) {
    assert.ok(handlers);

    [
      search,
      done
    ].forEach(function(h) {
      handlers.push(h);
    });

    return handlers;
  }

};

/*module.exports = function riak_search(req, res, next) {
  assert.ok(req.riak);
  assert.ok(req.riak.bucket);
  assert.ok(req.riak.db);
  assert.ok(req.riak.key);
  assert.ok(req.riak.log);

  var riak = req.riak;
  var bucket = req.riak.bucket;
  var db = req.riak.db;
  var indexes = req.riak.indexes;
  var indexesBucket = req.riak.indexesBucket;
  var key = req.riak.key;
  var log = req.riak.log;
  var rdns = req.riak.rdns;
  var parentKey = req.riak.parentKey;

  switch (req.scope) {
  case ldap.SCOPE_BASE_OBJECT:
    return db.get(bucket, key, function(err, obj, meta) {
      if (err) {
        log.debug('%s::%d err => %s', err.toString());
        res.end((err.statusCode === 404 ?
                ldap.LDAP_NO_SUCH_OBJECT :
                 ldap.LDAP_OPERATIONS_ERROR));
        return next();
      }

      // TODO check the search filter.
      sendSearchEntry(req, res, obj);
      res.end(ldap.LDAP_SUCCESS);
      return next();
    });

  case ldap.SCOPE_ONE_LEVEL:
    // TODO
  case ldap.SCOPE_SUBTREE:
    // TODO shoot the client if the filter isn't for indexed attrs
    // TODO equality search
    //    var indexKey = req.filter.attribute + '_';
    //if (req.filter.type === ldap.FILTER_EQUALITY)
    var job;
    switch (req.filter.type) {

    case ldap.FILTER_EQUALITY:
      var indexKey = encodeURIComponent(req.filter.value) + '_';
      job = db.add({
        bucket: bucket + '_' + req.filter.attribute,
        key_filters: [
          ['and', [['starts_with', indexKey]], [['ends_with', key]]]
        ]
      }).link({
        bucket: bucket,
        keep: false
      });
      break;

    case ldap.FILTER_PRESENT:
      job = db.add({
        bucket: bucket + '_' + req.filter.attribute,
        key_filters: [
          ['ends_with', key]
        ]
      }).link({
        bucket: bucket,
        keep: false
      });
      break;
    }
    assert.ok(job);

    job.map({
      language: 'erlang',
      module: 'riak_kv_mapreduce',
      'function': 'map_object_value'
    }).reduce({
      language: 'erlang',
      module: 'riak_kv_mapreduce',
      'function': 'reduce_set_union'
    }).reduce({
      language: 'erlang',
      module: 'riak_kv_mapreduce',
      'function': 'reduce_sort'
    });

    if (log.isDebugEnabled())
      log.debug('%s: search generated: %s',
                req.logId, util.inspect(job, false, 10));


    return job.run({
      timeout: req.timeLimit * 1000 || 10000
    }, function(err, data) {
      if (err) {
        try {
          if (JSON.parse(err.message.match(/\{.* /)).error === 'timeout') {
            res.end(ldap.LDAP_TIME_LIMIT_EXCEEDED);
            return next();
          }
        } catch (e) {}
        log.warn('%s riak failure:  %s', req.logId, err.stack);
        res.end(ldap.LDAP_OPERATIONS_ERROR);
        return next();
      } // end if (err)

      if (log.isDebugEnabled())
        log.debug('%s: riak search returned: %s',
                  req.logId, util.inspect(data));

      data.forEach(function(d) {
        try {
          var obj = JSON.parse(d);
          sendSearchEntry(req, res, obj);
        } catch (e) {
          log.error('%s: Invalid JSON: %s', req.logId, e);
        }

      });

      res.end(ldap.LDAP_SUCCESS);
      return next();
      });

  default:
    log.warn('%s: search scope 0x%s is invalid',
             req.logId, req.scope.toString(16));
    res.end(ldap.LDAP_PROTOCOL_ERROR);
    return next();
  }
};
*/
