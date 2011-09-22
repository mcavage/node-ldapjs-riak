// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var ldap = require('ldapjs');
var log4js = require('log4js');
var test = require('tap').test;
var uuid = require('node-uuid');



///--- Globals

var SUFFIX = 'o=' + uuid();
var SOCKET = '/tmp/.' + uuid();
var TOTAL_ENTRIES = 2;

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
      uid: true
    },
    client: {
      url: 'http://localhost:8098'
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
  server.del(SUFFIX, backend, backend.del());

  server.listen(SOCKET, function() {
    client = ldap.createClient({
      socketPath: SOCKET
    });
    t.ok(client);
    t.end();
  });
});


test('handler chain', function(t) {
  var handlers = backend.del();
  t.ok(handlers);
  t.ok(Array.isArray(handlers));
  handlers.forEach(function(h) {
    t.equal(typeof(h), 'function');
  });
  t.end();
});


test('handler chain append', function(t) {
  var handlers = backend.del([
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

    var finished = 0;
    for (var i = 0; i < TOTAL_ENTRIES; i++) {
      var entry = {
        cn: 'child' + i,
        objectClass: 'person',
        uid: uuid(),
        sn: 'test',
        l: i % 3 ? 'vancouver' : 'seattle'
      };
      client.add('cn=child' + i + ',' + SUFFIX, entry, function(err, res) {
        t.ifError(err);
        t.ok(res);
        t.equal(res.status, 0);

        if (++finished === TOTAL_ENTRIES)
          t.end();
      });
    }
  });
});


test('delete ok', function(t) {
  client.del('cn=child1,' + SUFFIX, function(err) {
    t.ifError(err);
    t.end();
  });
});


test('delete non-existent entry', function(t) {
  client.del('cn=child1,' + SUFFIX, function(err) {
    t.ok(err);
    t.ok(err instanceof ldap.NoSuchObjectError);
    t.end();
  });
});


test('delete non-leaf entry', function(t) {
  client.del(SUFFIX, function(err) {
    t.ok(err);
    t.ok(err instanceof ldap.NotAllowedOnNonLeafError);
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
