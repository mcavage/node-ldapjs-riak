// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var ldap = require('ldapjs');
var log4js = require('log4js');
var test = require('tap').test;
var uuid = require('node-uuid');



///--- Globals

var SUFFIX = 'cn=unit, o=test';
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
    indexes: {
      l: false,
      cn: true
    },
    client: {
      url: 'http://localhost:8098'
    },
    log4js: log4js
  });
  t.ok(backend);
  t.ok(backend.add);
  t.equal(typeof(backend.add), 'function');
  server = ldap.createServer();
  t.ok(server);

  server.add(SUFFIX, backend, backend.add());

  server.listen(SOCKET, function() {
    client = ldap.createClient({
      socketPath: SOCKET
    });
    t.ok(client);
    t.end();
  });
});


test('handler chain', function(t) {
  var handlers = backend.add();
  t.ok(handlers);
  t.ok(Array.isArray(handlers));
  handlers.forEach(function(h) {
    t.equal(typeof(h), 'function');
  });
  t.end();
});


test('handler chain append', function(t) {
  var handlers = backend.add([
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


test('add suffix', function(t) {
  var entry = {
    cn: 'unit',
    objectClass: 'organization',
    o: 'test'
  };
  client.add(SUFFIX, entry, function(err, res) {
    t.ifError(err);
    t.ok(res);
    t.equal(res.status, 0);
    t.end();
  });
});


test('add child missing parent', function(t) {
  var entry = {
    cn: 'unit',
    objectClass: 'organization',
    o: 'test'
  };
  client.add('cn=fail, ou=fail' + SUFFIX, entry, function(err, res) {
    t.ok(err);
    t.ok(err instanceof ldap.NoSuchObjectError);
    t.notOk(res);
    t.end();
  });
});


test('add child ok', function(t) {
  var entry = {
    cn: 'child',
    objectClass: 'person',
    sn: 'test',
    l: 'seattle'
  };
  client.add('cn=child,' + SUFFIX, entry, function(err, res) {
    t.ifError(err);
    t.ok(res);
    t.equal(res.status, 0);
    t.end();
  });
});


test('add child exists', function(t) {
  var entry = {
    objectClass: uuid()
  };
  client.add('cn=child,' + SUFFIX, entry, function(err, res) {
    t.ok(err);
    t.ok(err instanceof ldap.EntryAlreadyExistsError);
    t.notOk(res);
    t.end();
  });
});


test('add child unique confilct', function(t) {
  var entry = {
    cn: 'child',
    objectClass: 'person',
    sn: 'test',
    l: 'seattle'
  };
  client.add('cn=child2,' + SUFFIX, entry, function(err, res) {
    t.ok(err);
    t.ok(err instanceof ldap.ConstraintViolationError);
    t.notOk(res);
    t.end();
  });
});


test('teardown', function(t) {
  var riak = backend.client;
  var bucket = backend.bucket;

  function close() {
    client.unbind(function() {
      server.on('close', function() {
        t.end();
      });
      server.close();
    });
  }

  function removeUniqueIndexes() {
    var bucket = backend.uniqueIndexBucket.name;
    riak.list(bucket, function(err, keys) {
      if (keys && keys.length) {
        var finished = 0;
        keys.forEach(function(k) {
          riak.del(bucket, k, function(err) {
            if (++finished >= keys.length) {
              return close();
            }
          });
        });
      } else {
        return close();
      }
    });
  }

  var bucket = backend.bucket.name;
  return riak.list(bucket, function(err, keys) {
    if (keys && keys.length) {
      var finished = 0;
      keys.forEach(function(k) {
        riak.del(bucket, k, function(err) {
          if (++finished >= keys.length) {
            return removeUniqueIndexes();
          }
        });
      });
    } else {
      return removeUniqueIndexes();
    }
  });
});
