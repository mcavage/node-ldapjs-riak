var ldap = require('ldapjs');
var log4js = require('log4js');
var test = require('tap').test;
var uuid = require('node-uuid');



///--- Globals

var Attribute = ldap.Attribute;
var Change = ldap.Change;

var SUFFIX = 'o=' + uuid();
var SOCKET = '/tmp/.' + uuid();
var TOTAL_ENTRIES = 2;

var backend;
var addclient;
var client;
var server;

var ctrl = new ldap.PersistentSearchControl({
  type: '2.16.840.1.113730.3.4.3',
  value: {
    changeTypes: 15,
    changesOnly: false,
    returnECs: true
  }
});

var addOnly = new ldap.PersistentSearchControl({
  type: '2.16.840.1.113730.3.4.3',
  value: {
    changeTypes: 1,
    changesOnly: true,
    returnECs: true
  }
});

var deleteOnly = new ldap.PersistentSearchControl({
  type: '2.16.840.1.113730.3.4.3',
  value: {
    changeTypes: 2,
    changesOnly: true,
    returnECs: true
  }
});

var modOnly = new ldap.PersistentSearchControl({
  type: '2.16.840.1.113730.3.4.3',
  value: {
    changeTypes: 4,
    changesOnly: true,
    returnECs: true
  }
});

var changesOnly = new ldap.PersistentSearchControl({
  type: '2.16.840.1.113730.3.4.3',
  value: {
    changeTypes: 15,
    changesOnly: true,
    returnECs: true
  }
});



///--- Tests

test('setup', function(t) {
  // log4js.setGlobalLogLevel('Warn');
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

  // backend.log.setLevel('Warn');
  t.ok(backend);
  t.ok(backend.search);
  t.equal(typeof(backend.search), 'function');
  server = ldap.createServer();
  t.ok(server);

  server.add(SUFFIX, backend, backend.add());
  server.modify(SUFFIX, backend, backend.modify());
  server.search(SUFFIX, backend, backend.search());
  server.search('cn=changelog', backend, backend.changelogSearch());
  server.del(SUFFIX, backend, backend.del());

  server.listen(SOCKET, function() {
    client = ldap.createClient({
      socketPath: SOCKET
    });
    t.ok(client);
    addclient = ldap.createClient({
      socketPath: SOCKET
    });
    t.ok(addclient);
    t.end();
  });
});


test('add fixtures', function(t) {
  var suffix = {
    objectClass: ['top', 'organization'],
    o: SUFFIX.split('=')[1]
  };
  addclient.add(SUFFIX, suffix, function(err, res) {
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
      addclient.add('cn=child' + i + ',' + SUFFIX, entry, function(err, res) {
        t.ifError(err);
        if (err) {
          t.fail('error adding fixtures', err);
        }
        t.ok(res);
        t.equal(res.status, 0);
        console.log('add', finished);
        if (++finished === TOTAL_ENTRIES) {
          console.log('ending add fixtures');
          setTimeout(function() { t.end(); }, 1000);
        }
      });
    }
  });
});


test('persistent search', function(t) {
  console.log('entering search test');
  // sub search on a child cn
  client.search('cn=child1,' + SUFFIX, {scope: 'sub'}, ctrl,
  function(err, res) {
    t.ifError(err);
    t.ok(res);
    var retrieved = 0;
    res.on('searchEntry', function(entry) {
      retrieved++;
      if (retrieved > 2) {
        t.fail('only two entries for child 1');
      }
      t.ok(entry);
      t.ok(entry instanceof ldap.SearchEntry);
      t.ok(entry.dn.toString());
      t.equal(entry.dn.toString(), 'cn=child1, ' + SUFFIX);
      t.ok(entry.attributes);
      t.ok(entry.attributes.length);
      t.ok(entry.object);

      if (retrieved === 2) {
        t.ok(entry.controls[0]);
        t.ok(entry.controls[0].value.changeNumber);
        t.equal(entry.controls[0].value.changeType, 4);
      }
    });

    res.on('error', function(err) {
      t.fail('child1', err);
    });
    res.on('end', function(res) {
      t.fail('server should not sever connection');
    });
  });

  // search on changelog
  client.search('cn=changelog', { scope: 'sub'}, ctrl, function(err, res) {
    t.ifError(err);
    t.ok(res);
    var retrieved = 0;
    res.on('searchEntry', function(entry) {
      retrieved++;
      t.ok(entry);
      t.ok(entry instanceof ldap.SearchEntry);
      t.ok(entry.dn.toString());
      t.ok(entry.attributes);
      t.ok(entry.attributes.length);
      t.ok(entry.object);
    });
    res.on('error', function(err) {
      t.fail(err);
    });
    res.on('end', function(res) {
      t.fail('server should not sever connection');
    });
  });

  // search on suffix, return only dels
  client.search(SUFFIX, {scope: 'sub'}, deleteOnly, function(err, res) {
    t.ifError(err);
    t.ok(res);
    var retrieved = 0;
    res.on('searchEntry', function(entry) {
      retrieved++;
      t.ok(entry);
      t.ok(entry instanceof ldap.SearchEntry);
      t.ok(entry.dn.toString());
      t.ok(entry.attributes);
      t.ok(entry.attributes.length);
      t.ok(entry.object);
      if (retrieved === 1) {
        t.equal(entry.dn.toString(), 'cn=yunong, ' + SUFFIX);
        t.ok(entry.controls[0]);
        t.ok(entry.controls[0].value.changeNumber);
        t.equal(entry.controls[0].value.changeType, 2);
      }

      if (retrieved > 1) {
        t.fail('should only have 1 responses');
      }
    });
    res.on('error', function(err) {
      t.fail(err);
    });
    res.on('end', function(res) {
      t.fail('server should not sever connection');
    });
  });

  // search on suffix, return only adds
  client.search(SUFFIX, {scope: 'sub'}, addOnly, function(err, res) {
    t.ifError(err);
    t.ok(res);
    var retrieved = 0;
    res.on('searchEntry', function(entry) {
      retrieved++;
      t.ok(entry);
      t.ok(entry instanceof ldap.SearchEntry);
      t.ok(entry.dn.toString());
      t.ok(entry.attributes);
      t.ok(entry.attributes.length);
      t.ok(entry.object);
      if (retrieved === 1) {
        t.equal(entry.dn.toString(), 'cn=yunong, ' + SUFFIX);
        t.ok(entry.controls[0]);
        t.ok(entry.controls[0].value.changeNumber);
        t.equal(entry.controls[0].value.changeType, 1);
      }

      if (retrieved > 1) {
        t.fail('should only have 1 responses');
      }
    });
    res.on('error', function(err) {
      t.fail(err);
    });
    res.on('end', function(res) {
      t.fail('server should not sever connection');
    });
  });

  // search on suffix, return only mods
  client.search(SUFFIX, {scope: 'sub'}, modOnly, function(err, res) {
    t.ifError(err);
    t.ok(res);
    var retrieved = 0;
    res.on('searchEntry', function(entry) {
      retrieved++;
      t.ok(entry);
      t.ok(entry instanceof ldap.SearchEntry);
      t.ok(entry.dn.toString());
      t.ok(entry.attributes);
      t.ok(entry.attributes.length);
      t.ok(entry.object);
      if (retrieved === 1) {
        t.equal(entry.dn.toString(), 'cn=child1, ' + SUFFIX);
        t.ok(entry.controls[0]);
        t.ok(entry.controls[0].value.changeNumber);
        t.equal(entry.controls[0].value.changeType, 4);
      }

      if (retrieved > 1) {
        t.fail('should only have 1 responses');
      }
    });
    res.on('error', function(err) {
      t.fail(err);
    });
    res.on('end', function(res) {
      t.fail('server should not sever connection');
    });
  });

  // base search on suffix
  client.search(SUFFIX, {scope: 'base'}, ctrl, function(err, res) {
    t.ifError(err);
    t.ok(res);
    var retrieved = 0;
    res.on('searchEntry', function(entry) {
      retrieved++;
      if (retrieved > 1) {
        t.fail('should only have 1 entry on base');
      }
      t.ok(entry);
      t.ok(entry instanceof ldap.SearchEntry);
      t.ok(entry.dn.toString());
      t.ok(entry.attributes);
      t.ok(entry.attributes.length);
      t.ok(entry.object);
    });
    res.on('error', function(err) {
      t.fail(err);
    });
    res.on('end', function(res) {
      t.fail('server should not sever connection');
    });
  });


  // search on cn=child0, changes only while adding/deleting/modding
  client.search('cn=child0,' + SUFFIX, {scope: 'sub'}, changesOnly,
  function(err, res) {
    t.ifError(err);
    t.ok(res);
    res.on('searchEntry', function(entry) {
      t.fail('changesonly control should not fire');
    });
    res.on('error', function(err) {
      t.fail(err);
    });
    res.on('end', function(res) {
      t.fail('server should not sever connection');
    });
  });


  // search on suffix, changes only while adding/deleting/modding
  client.search(SUFFIX, {scope: 'sub'}, changesOnly, function(err, res) {
    t.ifError(err);
    t.ok(res);
    var retrieved = 0;
    res.on('searchEntry', function(entry) {
      retrieved++;
      console.log('changesonly all', retrieved);
      t.ok(entry);
      t.ok(entry instanceof ldap.SearchEntry);
      t.ok(entry.dn.toString());
      t.ok(entry.attributes);
      t.ok(entry.attributes.length);
      t.ok(entry.object);
      if (retrieved === 1) {
        t.equal(entry.dn.toString(), 'cn=yunong, ' + SUFFIX);
        t.ok(entry.controls[0]);
        t.ok(entry.controls[0].value.changeNumber);
        t.equal(entry.controls[0].value.changeType, 1);
      }

      if (retrieved === 2) {
        t.equal(entry.dn.toString(), 'cn=child1, ' + SUFFIX);
        t.ok(entry.controls[0]);
        t.equal(entry.controls[0].value.changeType, 4);
      }

      if (retrieved === 3) {
        t.equal(entry.dn.toString(), 'cn=yunong, ' + SUFFIX);
        t.ok(entry.controls[0]);
        t.ok(entry.controls[0].value.changeNumber);
        t.equal(entry.controls[0].value.changeType, 2);
      }

      if (retrieved > 3) {
        console.log('more than 4');
        t.fail('should only have 3 responses');
        t.end();
      }
    });
    res.on('error', function(err) {
      t.fail(err);
    });
    res.on('end', function(res) {
      t.fail('server should not sever connection');
    });
  });

  // search on suffix while adding/deleting/modding
  client.search(SUFFIX, {scope: 'sub'}, ctrl, function(err, res) {
    t.ifError(err);
    t.ok(res);
    var retrieved = 0;
    res.on('searchEntry', function(entry) {
      retrieved++;
      t.ok(entry);
      t.ok(entry instanceof ldap.SearchEntry);
      t.ok(entry.dn.toString());
      t.ok(entry.attributes);
      t.ok(entry.attributes.length);
      t.ok(entry.object);
      if (retrieved === TOTAL_ENTRIES + 2) {
        t.equal(entry.dn.toString(), 'cn=yunong, ' + SUFFIX);
        t.ok(entry.controls[0]);
        t.ok(entry.controls[0].value.changeNumber);
        t.equal(entry.controls[0].value.changeType, 1);
      }

      if (retrieved === TOTAL_ENTRIES + 3) {
        t.equal(entry.dn.toString(), 'cn=child1, ' + SUFFIX);
        t.ok(entry.controls[0]);
        t.equal(entry.controls[0].value.changeType, 4);
      }

      if (retrieved === TOTAL_ENTRIES + 4) {
        t.equal(entry.dn.toString(), 'cn=yunong, ' + SUFFIX);
        t.ok(entry.controls[0]);
        t.ok(entry.controls[0].value.changeNumber);
        t.equal(entry.controls[0].value.changeType, 2);
        t.end();
      }
    });
    res.on('error', function(err) {
      t.fail(err);
      if (err) {
        console.log('error', err);
      }
    });
    res.on('end', function(res) {
      t.fail('server should not sever connection');
    });
  });

  var entry = {
    cn: 'yunong',
    objectClass: 'person',
    uid: uuid(),
    sn: 'test',
    l: 'seattle'
  };

  var change = new Change({
    type: 'add',
    modification: {
      'pets': ['honey badger', 'bear']
    }
  });

  addclient.add('cn=yunong,' + SUFFIX, entry, function(err, res) {
    t.ifError(err);
    if (err)
    t.fail(err);

    addclient.modify('cn=child1,' + SUFFIX, change, function(err, res) {
      t.ifError(err);
      if (err)
      t.fail(err);

      addclient.del('cn=yunong,' + SUFFIX, function(err) {
        t.ifError(err);
        if (err)
        t.fail(err);
      });
    });
  });
});


test('teardown', function(t) {
  function close() {
    client.unbind(function() {
      addclient.unbind(function() {
        server.on('close', function() {
          console.log('closing server');
          t.end();
        });
        server.close();
      });
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
