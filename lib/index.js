// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');
var util = require('util');

var ldap = require('ldapjs');
var riak = require('riak-js');

var add = require('./add');
var bind = require('./bind');
var compare = require('./compare');
var del = require('./del');
var modify = require('./modify');
var modifyDN = require('./moddn');
var search = require('./search');



///--- APIs

/**
 * Constructs a new Riak backend for ldapjs.
 *
 * Options takes:
 *  - host
 *  - port
 *  - bucket
 *  - log4js
 *  - numConnections
 *  - indexes [array]
 *  - uniqueIndexes [array]
 *
 * @param {Object} options configuration object.
 * @throws {TypeError} on bad input.
 */
function RiakBackend(options) {
  if (!options || typeof(options) !== 'object')
    throw new TypeError('options (object) required');
  if (typeof(options.host) !== 'string')
    throw new TypeError('options.host (string) required');
  if (typeof(options.port) !== 'number')
    throw new TypeError('options.port (number) required');
  if (typeof(options.bucket) !== 'string')
    throw new TypeError('options.bucket (string) required');

  if (options.log4js && typeof(options.log4js) !== 'object')
    throw new TypeError('options.log4s must be an object');
  if (options.numConnections && typeof(options.numConnections) !== 'number')
    throw new TypeError('options.numConnections must be a number');


  if (options.indexes) {
    if (!Array.isArray(options.indexes))
      throw new TypeError('options.indexes must be an array[string]');
  } else {
    options.indexes = [];
  }
  if (options.uniqueIndexes) {
    if (!Array.isArray(options.uniqueIndexes))
      throw new TypeError('options.uniqueIndexes must be an array[string]');
  } else {
    options.uniqueIndexes = [];
  }

  var self = this;

  this.bucket = options.bucket;
  this.host = options.host;
  this.indexes = [];
  this.indexesBucket = options.bucket + '_indexes';
  this.log4js = options.log4js || ldap.log4js;
  this.numConnections = Math.abs(options.numConnections) || 3;
  this.port = options.port;
  this.retryConfig = options.retryConfig || {
    retries: 2,
    factor: 2,
    minTimeout: 500,
    maxTimeout: 3000
  };
  this.uniqueIndexes = [];
  this.uniqueIndexesBucket = options.bucket + '_unique_indexes';

  options.indexes.forEach(function(i) {
    if (typeof(i) !== 'string')
      throw new TypeError('options.indexes must be an array[string]');
    self.indexes.push(i.toLowerCase());
  });

  options.uniqueIndexes.forEach(function(i) {
    if (typeof(i) !== 'string')
      throw new TypeError('options.uniqueIndexes must be an array[string]');
    self.uniqueIndexes.push(i.toLowerCase());
  });

  this.connectionsIndex = -1;
  this.connections = [];

  this.log = this.log4js.getLogger('RiakBackend');

  this.__defineGetter__('name', function() { return 'RiakBackend'; });

  this.__defineGetter__('db', function() {
    if (++self.connectionsIndex >= self.numConnections)
      self.connectionsIndex = 0;

    if (!self.connections[self.connectionsIndex])
      self.connections[self.connectionsIndex] = riak.getClient({
        host: self.host,
        port: self.port,
        debug: self.log.isTraceEnabled()
      });

    return self.connections[self.connectionsIndex];
  });
}


RiakBackend.prototype.toString = function() {
  var self = this;
  return this.name + ': ' + JSON.stringify({
    host: self.host,
    port: self.port,
    bucket: self.bucket,
    indexes: self.indexes,
    uniqueIndexes: self.uniqueIndexes,
    connectionPoolSize: self.numConnections
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
      bucket: self.bucket,
      db: self.db,
      indexes: self.indexes,
      indexesBucket: self.indexesBucket,
      key: encodeURIComponent(req.dn.toString()),
      log: self.log4js.getLogger('Riak' + req.type),
      retryConfig: self.retryConfig,
      uniqueIndexes: self.uniqueIndexes,
      uniqueIndexesBucket: self.uniqueIndexesBucket
    };

    if (self.log.isDebugEnabled())
      self.log.debug('%s: riak setup: %s', req.logId, util.inspect(req.riak));

    return next();
  };
};



///--- Exported API

module.exports = {

  createBackend: function(options) {
    return new RiakBackend(options);
  },

  RiakBackend: RiakBackend
};
