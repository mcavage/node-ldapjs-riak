// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var ldap = require('ldapjs');
var log4js = require('log4js');
var test = require('tap').test;
var uuid = require('node-uuid');



///--- Globals

var SUFFIX = 'o=' + uuid();
var SOCKET = '/tmp/.' + uuid();
var TOTAL_ENTRIES = 3;

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
      url: 'http://localhost:8098'
    },
    log4js: log4js
  });
  t.ok(backend);
  t.ok(backend.search);
  t.equal(typeof(backend.search), 'function');
  server = ldap.createServer();
  t.ok(server);

  server.add(SUFFIX, backend, backend.add());
  server.search('cn=changelog', backend, backend.changelogSearch());

  server.listen(SOCKET, function() {
    client = ldap.createClient({
      socketPath: SOCKET
    });
    t.ok(client);
    t.end();
  });
});


test('handler chain', function(t) {
  var handlers = backend.changelogSearch();
  t.ok(handlers);
  t.ok(Array.isArray(handlers));
  handlers.forEach(function(h) {
    t.equal(typeof(h), 'function');
  });
  t.end();
});


test('handler chain append', function(t) {
  var handlers = backend.changelogSearch([
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


test('search sub objectclass=*', function(t) {
  client.search('cn=changelog', { scope: 'sub' }, function(err, res) {
    t.ifError(err);
    t.ok(res);

    var retrieved = 0;
    res.on('searchEntry', function(entry) {
      t.ok(entry);
      t.ok(entry instanceof ldap.SearchEntry);
      t.ok(entry.dn.toString());
      t.ok(entry.attributes);
      t.ok(entry.attributes.length);
      t.ok(entry.object);
      retrieved++;
    });
    res.on('error', function(err) {
      t.fail(err);
    });
    res.on('end', function(res) {
      t.ok(res);
      t.ok(res instanceof ldap.SearchResponse);
      t.equal(res.status, 0);
      t.ok(retrieved);
      t.end();
    });
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
