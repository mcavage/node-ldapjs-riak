// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');

var ldap = require('ldapjs');

var common = require('./common');



///--- Globals

var operationsError = common.operationsError;
var parseDN = ldap.parseDN;



///--- Handlers

function _send(req, res, obj) {
  var dn = parseDN(obj.dn);
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
    if ((req.dn.parentOf(dn) || req.dn.equals(dn)) && req.filter.matches(attrs))
      send = true;
    break;
  }

  if (send) {
    req.riak.log.debug('%s sending: %j', req.logId, obj);
    res.send(obj, req.hidden);
  }
}


///--- API

function subtreeSearch(req, res, next) {
  var bucket = req.riak.bucket;
  var client = req.riak.client;
  var filter = req.filter;
  var key = req.riak.key;
  var log = req.riak.log;

  function getIndexVals(filter) {
    assert.ok(filter);

    if (filter.attribute && req.riak.indexes.indexOf(filter.attribute) === -1)
      return false;

    var vals = false;
    switch (filter.type) {
    case 'present':
      vals = [String.fromCharCode(0x00), String.fromCharCode(0xffff)];
      break;

    case 'approx':
    case 'equal':
      vals = [filter.value];
      break;

    case 'substring':

      function _substring(value) {
        if (value) {
          vals = [];
          vals.push(value);
          vals.push(value + String.fromCharCode(0xffff));
          return true;
        }

        return false;
      }

      if (_substring(filter.initial))
        break;

      if (filter.any.length)
        if (_substring(filter.any[0]))
          break;

      if (filter['final'])
        if (_substring(filter['final']))
          break;

      break;

    case 'ge':
      vals = [filter.value, String.fromCharCode(0xffff)];
      break;

    case 'le':
      vals = [String.fromCharCode(0x00), filter.value];
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

      objects.forEach(function(o) {
        _send(req, res, o);
      });
      return next();
    });
  }

  return client.listKeys(bucket, function(err, keys) {
    if (err)
      return next(operationsError(err));

    if (!keys.length)
      return next();

    var queried = 0;
    var done = false;
    var finished = 0;
    keys.forEach(function(k) {
      var dn = parseDN(k);
      if (!req.baseObject.parentOf(dn) && !req.baseObject.equals(dn))
        return;

      queried++;
      return client.get(bucket, k, function(err, obj) {
        if (err && !done) {
          done = true;
          return next(operationsError(err));
        }

        _send(req, res, obj);

        if (++finished === queried && !done) {
          done = true;
          return next();
        }
      });
    });
    if (!queried)
      return next();
  });
}


function baseSearch(req, res, next) {
  var log = req.riak.log;
  var bucket = req.riak.bucket;
  var client = req.riak.client;
  var key = req.riak.key;

  return client.get(bucket, key, function(err, obj) {
    if (err) {
      if (err.code !== 404)
        return next(operationsError(err));
      return next(new ldap.NoSuchObjectError(key));
    }

    _send(req, res, obj);
    return next();
  });
}


function search(req, res, next) {
  var log = req.riak.log;

  if (log.isDebugEnabled())
    log.debug('%s searching %j', req.logId, req.json);

  req.hidden = false;
  req.controls.forEach(function(c) {
    if (c.type === 'hidden')
      req.hidden = true;
  });

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
      common.done

    ].forEach(function(h) {
      handlers.push(h);
    });

    return handlers;
  }

};
