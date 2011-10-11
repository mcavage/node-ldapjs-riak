// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');
var util = require('util');

var ldap = require('ldapjs');
var uuid = require('node-uuid');

var add = require('./add');
var bind = require('./bind');
var compare = require('./compare');
var del = require('./del');
var modify = require('./modify');
var modifyDN = require('./moddn');
var search = require('./search');

var Riak = require('./riak');



///--- Backend

/**
 * Constructs a new Riak backend for ldapjs.
 *
 * Options takes:
 *  {
 *    "bucket": {
 *      "name": "ufds",
 *      "props": {      // These get set at startup
 *        "n_val": 1,
 *        "allow_mult": false,
 *        "last_write_wins": false,
 *        "r": 1,
 *        "w": "quorum",
 *        "dw": 1,
 *        "rw": "quorum"
 *      }
 *    },
 *    "indexes": {
 *      "login": true,  // true means unique index. false means not unique
 *      "email": true,
 *      "uuid": true,
 *      "fingerprint": false
 *    },
 *    "client": {
 *      "url": "http://localhost:8098",
 *      "clientId": "coal-dev",   // set this to `uname -n`
 *      "retry": {
 *        "retries": 3,
 *        "factor": 2,
 *        "minTimeout": 1000,
 *        "maxTimeout": 10000
 *      }
 *    },
 *    "log4js": $(configured log4js object)
 *  }
 *
 * @param {Object} options configuration object.
 * @throws {TypeError} on bad input.
 */
function RiakBackend(options) {
  if (!options || typeof(options) !== 'object')
    throw new TypeError('options (object) required');
  if (typeof(options.log4js) !== 'object')
    throw new TypeError('options.log4s (object) required');
  if (typeof(options.bucket) !== 'object')
    throw new TypeError('options.bucket (object) required');
  if (typeof(options.client) !== 'object')
    throw new TypeError('options.client (object) required');
  if (options.indexes && typeof(options.indexes) !== 'object')
    throw new TypeError('options.indexes must be an object');

  var self = this;

  this.log = options.log4js.getLogger('RiakBackend');
  this.indexes = [];
  this.uniqueIndexes = [];
  if (options.indexes) {
    Object.keys(options.indexes).forEach(function(i) {
      self.indexes.push(i);
      if (options.indexes[i]) {
        if (!options.uniqueIndexBucket)
          throw new Error('unique index set(' + i +
                          ') but no unique index bucket set in config');
        self.uniqueIndexes.push(i);
      }
    });
  }

  this.__defineGetter__('name', function() {
    return 'RiakBackend';
  });

  this.__defineGetter__('client', function() {
    if (!self._riak)
      self._riak = new Riak(options.client);

    return self._riak;
  });

  this.__defineGetter__('log4js', function() {
    return options.log4js;
  });

  this.__defineGetter__('bucket', function() {
    return options.bucket;
  });

  this.__defineGetter__('uniqueIndexBucket', function() {
    return options.uniqueIndexBucket || {};
  });
}


/**
 * Connects to Riak and performs SetBucket(config.properties).
 *
 * @param {Function} callback of the form f(err).
 */
RiakBackend.prototype.init = function(callback) {
  if (typeof(callback) !== 'function')
    throw new TypeError('callback (function) required');

  var client = this.client;
  function _init(bucket, _callback) {
    if (!bucket || !bucket.name || !bucket.props)
      return _callback(null);

    return client.setBucket(bucket.name, bucket.props, function(err) {
      return _callback(err);
    });
  }

  var self = this;
  return _init(this.bucket, function(err) {
    if (err)
      return callback(err);

    return _init(self.uniqueIndexBucket, function(err) {
      return callback(err);
    });
  });
};


RiakBackend.prototype.add = function(handlers) {
  return this._operation(add, handlers);
};

RiakBackend.prototype.bind = function(handlers) {
  return this._operation(bind, handlers);
};



RiakBackend.prototype.compare = function(handlers) {
  return this._operation(compare, handlers);
};


RiakBackend.prototype.del = function(handlers) {
  return this._operation(del, handlers);
};


RiakBackend.prototype.modify = function(handlers) {
  return this._operation(modify, handlers);
};


RiakBackend.prototype.modifyDN = function(handlers) {
  return this._operation(modifyDN, handlers);
};


RiakBackend.prototype.search = function(handlers) {
  return this._operation(search, handlers);
};


RiakBackend.prototype._operation = function(op, handlers) {
  if (!handlers)
    handlers = [];

  if (!Array.isArray(handlers))
    handlers = [handlers];
  handlers.unshift(this._setup());
  return op.chain(handlers);
};


RiakBackend.prototype._setup = function() {
  var self = this;
  return function setup(req, res, next) {
    req.riak = {
      bucket: self.bucket.name,
      client: self.client,
      indexes: self.indexes,
      key: req.dn.toString(),
      log: self.log4js.getLogger('Riak' + req.type),
      uniqueIndexBucket: self.uniqueIndexBucket.name,
      uniqueIndexes: self.uniqueIndexes
    };

    return next();
  };
};


RiakBackend.prototype.toString = function() {
  var self = this;
  return this.name + ': ' + JSON.stringify({
    url: self.client.url,
    bucket: self.bucket,
    indexes: self.indexes
  });
};


///--- Exported API

module.exports = {

  createBackend: function(options) {
    return new RiakBackend(options);
  },

  RiakBackend: RiakBackend
};
