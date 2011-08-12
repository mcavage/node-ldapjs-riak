// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');

var ldap = require('ldapjs');

var common = require('./common');



///--- Internal Helpers

function check(req, res, next) {
  if (req.version !== 3)
    return next(new ldap.ProtocolError(req.version + ' is not v3'));

  if (req.authentication !== 'simple')
    return next(new ldap.AuthMethodNotSupportedError(req.authentication));

  return next();
}


function load(req, res, next) {
  common.load(req, req.riak.bucket, req.riak.key, function(err, entry) {
    if (err)
      return next(err);

    if (!entry)
      return next(new ldap.NoSuchObjectError(req.dn.toString()));

    return next();
  });
}


function bind(req, res, next) {
  assert.ok(req.riak.entry);

  var obj = req.riak.entry.attributes;
  if (!obj.userpassword)
    return next(new ldap.NoSuchAttributeError('userPassword'));

  if (obj.userpassword[0] !== req.credentials)
    return next(new ldap.InvalidCredentialsError());

  res.end();
  return next();
}



///--- Exported API

module.exports = {

  chain: function(handlers) {
    assert.ok(handlers);

    [
      check,
      load,
      bind
    ].forEach(function(h) {
      handlers.push(h);
    });

    return handlers;
  }

};
