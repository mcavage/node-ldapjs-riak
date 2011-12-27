// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');
var qs = require('querystring');

var ldap = require('ldapjs');

var common = require('./common');



///--- Globals

var operationsError = common.operationsError;
var parseDN = ldap.parseDN;

var UTF8_START = String.fromCharCode(0x00);
var UTF8_END = String.fromCharCode(0xffff);


///--- Handlers

function _send(req, res, obj, callback) {
  var log = req.riak.log;

  // the dn of the object is used to determine whether we send the request.
  // for persiseent search, it's not the dn of the request that came in, but
  // rather the object
  var dn = req.persistentSearch ? obj.dn : req.dn;

  if (typeof(req.searchCallback) !== 'function')
    return common.sendSearchRequest(req, res, obj, dn, null, callback);

  log.debug('searchCallback registered, calling with %j', obj);
  return req.searchCallback(req, obj, function(err, _obj) {
    if (err)
      log.warn('%s searchCallback failed for %s. Sending original entry: %s',
               req.logId, req.dn.toString(), err.stack);

    return common.sendSearchRequest(req, res, _obj || obj, dn, null, callback);
  });
}


///--- API

function subtreeSearch(req, res, next) {
  var bucket = req.riak.bucket;
  var client = req.riak.client;
  var filter = req.filter;
  var key = req.riak.key;

  function getIndexVals(filter) {
    assert.ok(filter);

    if (filter.attribute && req.riak.indexes.indexOf(filter.attribute) === -1)
      return false;

    var vals = false;
    switch (filter.type) {
    case 'present':
    case 'substring':
      vals = [UTF8_START, UTF8_END];
      break;

    case 'approx':
    case 'equal':
      vals = [qs.escape(filter.value)];
      break;

    case 'ge':
      vals = [qs.escape(filter.value), UTF8_END];
      break;

    case 'le':
      vals = [UTF8_START, qs.escape(filter.value)];
      break;

    case 'and':
      for (var i = 0; i < filter.filters.length; i++) {
        var _res = getIndexVals(filter.filters[i]);
        if (_res && _res.attribute && _res.vals)
          return _res;
      }

      break;

    default: // or and not we can't deal with here
      break;
    }

    return vals ? { attribute: filter.attribute, vals: vals } : false;
  }

  var job = getIndexVals(req.filter);
  if (job) {
    return client.find(bucket, job.attribute, job.vals, function(err, objects) {
      if (err)
        return next(operationsError(err));

      if (!objects || !objects.length)
        return next();

      var finished = 0;
      function callback() {
        if (++finished === objects.length)
          return next();
      }

      objects.forEach(function(o) {
        _send(req, res, o, callback);
      });
    });
  }

  return client.listKeys(bucket, function(err, keys) {
    if (err)
      return next(operationsError(err));

    if (!keys.length)
      return next();

    var query = [];
    var done = false;
    var finished = 0;
    keys.forEach(function(k) {
      var dn = parseDN(k);

      if (!req.baseObject.parentOf(dn) && !req.baseObject.equals(dn))
        return;

      return query.push(k);
    });

    if (!query.length)
      return next();

    return query.forEach(function(k) {
      return client.get(bucket, k, function(err, obj) {
        if (done)
          return;

        if (err) {
          done = true;
          return next(operationsError(err));
        }

        return _send(req, res, obj, function() {
          if (++finished === query.length) {
            if (!done) {
              done = true;
              return next();
            }
          }
        });
      });
    });
  });
}


function baseSearch(req, res, next) {
  var bucket = req.riak.bucket;
  var client = req.riak.client;
  var key = req.riak.key;

  return client.get(bucket, key, function(err, obj) {
    if (err) {
      if (err.code !== 404)
        return next(operationsError(err));
      return next(new ldap.NoSuchObjectError(key));
    }

    _send(req, res, obj, function() {
      return next();
    });
  });
}


function oneLevelSearch(req, res, next) {
  var log = req.riak.log;
  var bucket = req.riak.bucket;
  var client = req.riak.client;
  var key = req.riak.key;

  return client.find(bucket, '_parent', qs.escape(key), function(err, objects) {
    if (err)
      return next(operationsError(err));

    if (!objects || !objects.length)
      return next();

    var finished = 0;
    function callback() {
      if (++finished === objects.length)
        return next();
    }

    objects.forEach(function(o) {
      _send(req, res, o, callback);
    });
  });
}


function search(req, res, next) {
  var log = req.riak.log;

  if (log.isDebugEnabled())
    log.debug('%s searching %j', req.logId, req.json);

  // intercept the search request and figure out if it's persistent
  req.controls.forEach(function(c) {
    if (c.type === '1.3.6.1.4.1.38678.1') // hidden attributes control
      req.hidden = true;
    if (c.type === '2.16.840.1.113730.3.4.3') { // persistent search control
      req.persistentSearch = c;
    }
  });

  if (req.persistentSearch && req.persistentSearch.value.changesOnly) {
    // short circuit search if it's changes only
      return next();
  }

  try {
    switch (req.scope) {
    case 'base':
      return baseSearch(req, res, next);
    case 'one':
      return oneLevelSearch(req, res, next);
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
  send: _send,
  chain: function(handlers) {
    assert.ok(handlers);

    [
      search,
      common.done

    ].forEach(function(h) {
      handlers.push(h);
    });

    return handlers;
  }

};
