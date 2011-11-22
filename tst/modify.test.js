// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var ldap = require('ldapjs');
var log4js = require('log4js');
var test = require('tap').test;
var uuid = require('node-uuid');



///--- Globals

var Attribute = ldap.Attribute;
var Change = ldap.Change;

var SUFFIX = 'o=' + uuid();
var SOCKET = '/tmp/.' + uuid();

var backend;
var client;
var server;



///--- Tests

test('setup', function(t) {
  var riakjs = require('../lib/index');
  t.ok(riakjs);
  t.ok(riakjs.createBackend);
  t.equal(typeof(riakjs.createBackend), 'function');
  backend = riakjs.createBackend({
    bucket: {
      name: uuid()
    },
    uniqueIndexBucket: {
      name: uuid()
    },
    changelogBucket: {
      name: uuid()
    },
    indexes: {
      l: false,
      uid: true
    },
    client: {
      url: 'http://localhost:8098',
      cache: {
        size: 100,
        age: 10
      }
    },
    log4js: log4js
  });
  t.ok(backend);
  t.ok(backend.add);
  t.equal(typeof(backend.add), 'function');
  server = ldap.createServer({
    log4js: log4js
  });
  t.ok(server);

  server.add(SUFFIX, backend, backend.add());
  server.modify(SUFFIX, backend, backend.modify());

  server.listen(SOCKET, function() {
    client = ldap.createClient({
      socketPath: SOCKET
    });
    t.ok(client);
    t.end();
  });
});


test('handler chain', function(t) {
  var handlers = backend.modify();
  t.ok(handlers);
  t.ok(Array.isArray(handlers));
  handlers.forEach(function(h) {
    t.equal(typeof(h), 'function');
  });
  t.end();
});


test('handler chain append', function(t) {
  var handlers = backend.modify([
    function foo(req, res, next) {
      return next();
    }
  ]);
  t.ok(handlers);
  t.ok(Array.isArray(handlers));
  handlers.forEach(function(h) {
    t.equal(typeof(h), 'function');
  });
  t.equal(handlers[1].name, 'foo');
  t.end();
});


test('add fixtures', function(t) {
  var suffix = {
    objectClass: 'top',
    objectClass: 'organization',
    o: SUFFIX.split('=')[1]
  };
  client.add(SUFFIX, suffix, function(err, res) {
    t.ifError(err);
    t.ok(res);
    t.equal(res.status, 0);
    t.end();
  });
});


test('modify add ok', function(t) {
  var change = new Change({
    type: 'add',
    modification: {
      'pets': ['honey badger', 'bear']
    }
  });
  client.modify(SUFFIX, change, function(err, res) {
    t.ifError(err);
    t.end();
  });
});


test('modify replace ok', function(t) {
  var change = new Change({
    type: 'replace',
    modification: new Attribute({
      type: 'pets',
      vals: ['moose']
    })
  });
  client.modify(SUFFIX, change, function(err, res) {
    t.ifError(err);
    t.end();
  });
});


test('modify delete ok', function(t) {
  var change = new Change({
    type: 'delete',
    modification: new Attribute({
      type: 'pets'
    })
  });
  client.modify(SUFFIX, change, function(err, res) {
    t.ifError(err);
    t.end();
  });
});


test('modify non-existent entry', function(t) {
  var change = new Change({
    type: 'delete',
    modification: new Attribute({
      type: 'pets'
    })
  });
  client.modify('cn=child1,' + SUFFIX, change, function(err) {
    t.ok(err);
    t.ok(err instanceof ldap.NoSuchObjectError);
    t.end();
  });
});


test('teardown', function(t) {
  function close() {
    client.unbind(function() {
      server.on('close', function() {
        t.end();
      });
      server.close();
    });
  }

  function cleanup(bucket) {
    riak.list(bucket, function(err, keys) {
      if (keys && keys.length) {
        var _finished = 0;
        return keys.forEach(function(k) {
          riak.del(bucket, k, function(err) {
            if (++_finished >= keys.length) {
              if (++finished === 3)
              return close();
            }
          });
        });
      }

      if (++finished === 3)
        return close();
    });
  }

  var riak = backend.client;
  var finished = 0;
  cleanup(backend.bucket.name);
  cleanup(backend.changelogBucket.name);
  cleanup(backend.uniqueIndexBucket.name);
});
