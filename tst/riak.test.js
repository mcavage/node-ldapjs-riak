// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var log4js = require('log4js');
var test = require('tap').test;
var uuid = require('node-uuid');

var Riak = require('../lib/riak');



///--- Globals

var client;
var bucket = uuid();
var key = uuid();



///--- Test

test('setup', function(t) {
  log4js.setGlobalLogLevel('TRACE');
  client = new Riak({
    log4js: log4js
  });
  t.ok(client);
  t.end();
});


test('constructor no options', function(t) {
  var _client = new Riak();
  t.ok(_client);
  t.end();
});


test('ListBuckets', function(t) {
  client.listBuckets(function(err, buckets) {
    t.ifError(err);
    t.ok(buckets);
    t.end();
  });
});


test('SetBucket', function(t) {
  client.setBucket(uuid(), { n_val: 1 }, function(err) {
    t.ifError(err);
    t.end();
  });
});


test('ListKeys', function(t) {
  client.list(uuid(), function(err, buckets) {
    t.ifError(err);
    t.ok(buckets);
    t.end();
  });
});


test('GetObject (404)', function(t) {
  client.get(bucket, uuid(), function(err, object) {
    t.ok(err);
    t.equal(err.name, 'NotFoundError');
    t.ok(!object);
    t.end();
  });
});


test('StoreObject (with key)', function(t) {
  client.put(bucket, key, {foo: 'bar'}, function(err, key, headers) {
    t.ifError(err);
    t.ok(key);
    t.end();
  });
});


test('FetchObject', function(t) {
  client.get(bucket, key, function(err, obj, headers) {
    t.ifError(err);
    t.ok(obj);
    t.equal(obj.foo, 'bar');
    t.end();
  });
});


test('DeleteObject', function(t) {
  client.del(bucket, key, function(err, headers) {
    t.ifError(err);
    t.ok(headers);
    t.end();
  });
});


test('GetObject (deleted)', function(t) {
  client.get(bucket, key, function(err, object) {
    t.ok(err);
    t.equal(err.name, 'NotFoundError');
    t.ok(!object);
    t.end();
  });
});


test('StoreObject (no key, indexes)', function(t) {
  var obj = {
    foo: 'bar',
    email: 'foo@bar.com'
  };
  var opts = {
    indexes: ['email']
  };
  client.put(bucket, key, obj, opts, function(err, key, headers) {
    t.ifError(err);
    t.ok(key);
    var obj2 = {
      foo: 'car',
      email: 'foo@car.com'
    };
    client.post(bucket, obj2, opts, function(err, key, headers) {
      t.ifError(err);
      t.ok(key);
      t.end();
    });
  });
});


test('Find by index', function(t) {
  client.find(bucket, 'email', 'foo@car.com', function(err, objects) {
    t.ifError(err);
    t.ok(objects);
    t.ok(objects.length);
    t.ok(objects[0]);
    t.equal(objects[0].email, 'foo@car.com');
    t.end();
  });
});


test('map reduce', function(t) {
  client.mapred(
    {
      bucket: bucket,
      key_filters: [['matches', '.*']]
    },
    [
      {
        reduce: {
          language: 'erlang',
          module: 'riak_kv_mapreduce',
          'function': 'reduce_identity',
          'keep': true
        }
      }
    ],
    function(err, stuff) {
      t.ifError(err);
      t.ok(stuff);
      t.end();
    });
});



test('tear down (via listKeys -> Delete)', function(t) {
  client.listKeys(bucket, function(err, keys, headers) {
    t.ifError(err);
    t.ok(keys);

    var finished = 0;
    return keys.forEach(function(k) {
      return client.del(bucket, k, function(err) {
        t.ifError(err);
        if (++finished === keys.length)
          t.end();
      });
    });
  });
});
