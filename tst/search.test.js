// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var ldap = require('ldapjs');
var log4js = require('log4js');
var test = require('tap').test;
var uuid = require('node-uuid');



///--- Globals

var SUFFIX = 'o=' + uuid();
var SOCKET = '/tmp/.' + uuid();
var TOTAL_ENTRIES = 100;

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
      url: 'http://localhost:8098',
      cache: {
        size: 100,
        age: 20
      }
    },
    log4js: log4js
  });
  t.ok(backend);
  t.ok(backend.search);
  t.equal(typeof(backend.search), 'function');
  server = ldap.createServer();
  t.ok(server);

  server.add(SUFFIX, backend, backend.add());
  server.search(SUFFIX, backend, backend.search());

  server.listen(SOCKET, function() {
    client = ldap.createClient({
      socketPath: SOCKET
    });
    t.ok(client);
    t.end();
  });
});


test('handler chain', function(t) {
  var handlers = backend.search();
  t.ok(handlers);
  t.ok(Array.isArray(handlers));
  handlers.forEach(function(h) {
    t.equal(typeof(h), 'function');
  });
  t.end();
});


test('handler chain append', function(t) {
  var handlers = backend.search([
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


test('search base objectclass=*', function(t) {
  client.search('cn=child1,' + SUFFIX, function(err, res) {
    t.ifError(err);
    t.ok(res);

    var retrieved = 0;
    res.on('searchEntry', function(entry) {
      t.ok(entry);
      t.ok(entry instanceof ldap.SearchEntry);
      t.equal(entry.dn.toString(), 'cn=child1, ' + SUFFIX);
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
      t.equal(retrieved, 1);
      t.end();
    });
  });
});


test('search base eq filter ok', function(t) {
  client.search('cn=child1,' + SUFFIX, '(cn=child1)', function(err, res) {
    t.ifError(err);
    t.ok(res);

    var retrieved = 0;
    res.on('searchEntry', function(entry) {
      t.ok(entry);
      t.ok(entry instanceof ldap.SearchEntry);
      t.equal(entry.dn.toString(), 'cn=child1, ' + SUFFIX);
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
      t.equal(retrieved, 1);
      t.end();
    });
  });
});


test('search base eq filter no match', function(t) {
  client.search('cn=child1,' + SUFFIX, '(cn=child2)', function(err, res) {
    t.ifError(err);
    t.ok(res);

    res.on('searchEntry', function(entry) {
      t.fail('Got an entry, but shouldn\'t have');
    });
    res.on('error', function(err) {
      t.fail(err);
    });
    res.on('end', function(res) {
      t.ok(res);
      t.ok(res instanceof ldap.SearchResponse);
      t.equal(res.status, 0);
      t.end();
    });
  });
});


test('search sub filter ok', function(t) {
  var opts = {
    filter: '(cn=child*)',
    scope: 'sub'
  };
  client.search(SUFFIX, opts, function(err, res) {
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
      t.equal(retrieved, TOTAL_ENTRIES); // suffix doesn't match
      t.end();
    });
  });
});


test('search sub filter no match', function(t) {
  var opts = {
    filter: '(foo=bar)',
    scope: 'sub'
  };
  client.search(SUFFIX, opts, function(err, res) {
    t.ifError(err);
    t.ok(res);

    res.on('searchEntry', function(entry) {
      t.fail('Got an entry, but shouldn\'t have');
    });
    res.on('error', function(err) {
      t.fail(err);
    });
    res.on('end', function(res) {
      t.ok(res);
      t.ok(res instanceof ldap.SearchResponse);
      t.equal(res.status, 0);
      t.end();
    });
  });
});


test('search sub ge filter ok', function(t) {
  var opts = {
    scope: 'sub',
    filter: '(cn>=child9)'
  };
  client.search(SUFFIX, opts, function(err, res) {
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
      t.equal(retrieved, 11);
      t.end();
    });
  });
});


test('search sub le filter ok', function(t) {
  var opts = {
    scope: 'sub',
    filter: '(cn<=child19)'
  };
  client.search(SUFFIX, opts, function(err, res) {
    t.ifError(err);
    t.ok(res);

    var retrieved = 0;
    res.on('searchEntry', function(entry) {
      t.ok(entry);
      t.ok(entry instanceof ldap.SearchEntry);
      console.log(entry.dn.toString());
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
      t.equal(retrieved, 12);
      t.end();
    });
  });
});


test('search sub and filter ok', function(t) {
  var opts = {
    scope: 'sub',
    filter: '(&(cn>=child19)(sn=test))'
  };
  client.search(SUFFIX, opts, function(err, res) {
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
      t.equal(retrieved, TOTAL_ENTRIES - 11);
      t.end();
    });
  });
});


test('search sub or filter ok', function(t) {
  var opts = {
    scope: 'sub',
    filter: '(|(cn>=child19)(sn=t*s*))'
  };
  client.search(SUFFIX, opts, function(err, res) {
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
      t.equal(retrieved, TOTAL_ENTRIES);
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
              if (++finished === 2)
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
  cleanup(backend.uniqueIndexBucket.name);
});
