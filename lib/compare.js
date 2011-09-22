// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');

var ldap = require('ldapjs');

var common = require('./common');



///--- Handlers

function compare(req, res, next) {
  assert.ok(req.riak.entry);

  var attribute = req.riak.entry.attributes[req.attribute];
  if (!attribute)
    return next(new ldap.NoSuchAttributeError(req.attribute));

  var found = false;
  for (var i = 0; i < attribute.length; i++) {
    if (req.value === attribute[i]) {
      found = true;
      break;
    }
  }

  res.end(found);
  return next();
}



///--- Exported API

module.exports = {

  chain: function(handlers) {
    assert.ok(handlers);

    [
      common.load,
      compare
    ].forEach(function(h) {
      handlers.push(h);
    });

    return handlers;
  }

};
