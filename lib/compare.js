// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');

var ldap = require('ldapjs');

var common = require('./common');



///--- Internal Helpers

function load(req, res, next) {
  common.load(req, req.riak.bucket, req.riak.key, function(err, entry) {
    if (err)
      return next(err);

    if (!entry)
      return next(new ldap.NoSuchObjectError(req.dn.toString()));

    return next();
  });
}


function compare(req, res, next) {
  assert.ok(req.riak.entry);

  var obj = req.riak.entry.attributes;

  if (!obj[req.attribute])
    return next(new ldap.NoSuchAttributeError(req.attribute));

  for (var i = 0; i < obj[req.attribute].length; i++) {
    if (req.value === obj[req.attribute][i]) {
      res.end(ldap.LDAP_COMPARE_TRUE);
      return next();
    }
  }

  return next(new ldap.CompareFalseError(req.attribute));
}


///--- Exported API

module.exports = {

  chain: function(handlers) {
    assert.ok(handlers);

    [
      load,
      compare
    ].forEach(function(h) {
      handlers.push(h);
    });

    return handlers;
  }

};
