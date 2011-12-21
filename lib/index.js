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
 *    "uniqueIndexBucket": {
 *      "name": "ufds_uindex",
 *      "props": {
 *        "n_val": 2,
 *        "allow_mult": false,
 *        "last_write_wins": false,
 *        "r": "all",
 *        "w": "all",
 *        "dw": 0,
 *        "rw": "all"
 *       }
 *    },
 *    "changelogBucket": {
 *      "name": "ufds_changelog",
 *      "props": { ... },
 *      "suffix": "dc=changelog",
 *      "sequenceCallback": function() {
 *                            return new Date().getTime() + '';
 *      }
 *    },
 *    "indexes": {
 *      "login": true,  // true means unique index. false means not unique
 *      "email": false,
 *      "uuid": true,
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
    throw new TypeError('options.log4js (object) required');
  if (typeof(options.bucket) !== 'object')
    throw new TypeError('options.bucket (object) required');
  if (typeof(options.client) !== 'object')
    throw new TypeError('options.client (object) required');
  if (options.indexes && typeof(options.indexes) !== 'object')
    throw new TypeError('options.indexes must be an object');
  if (options.changelogBucket) {
    var clb = options.changelogBucket;

    if (typeof(clb) !== 'object')
      throw new TypeError('options.changelogBucket must be an object');
    if (clb.changeNumberCallback &&
        typeof(clb.changeNumberCallback) !== 'function')
      throw new TypeError('sequenceCallback must be a function');

    if (!clb.changeNumberCallback) {
      clb.changeNumberCallback = function() {
        return new Date().getTime() + '';
      };
    }

    if (!clb.suffix)
      clb.suffix = 'cn=changelog';
    clb.__defineGetter__('nextChangeNumber', function() {
      var n = clb.changeNumberCallback() + '';
      return {
        dn: 'changenumber=' + n + ', ' + clb.suffix,
        changeNumber: n
      };
    });

    clb.__defineGetter__('indexes', function() {
      return ['targetdn',
              'changenumber_int',
              'changetime',
              'changetype'];
    });

  }

  var self = this;

  this.log = options.log4js.getLogger('RiakBackend');
  this.indexes = ['_parent'];
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
    if (!self._riak) {
      options.client.log4js = options.log4js;
      self._riak = new Riak(options.client);
    }


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

  this.__defineGetter__('changelogBucket', function() {
    return options.changelogBucket || {};
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

  var finished = 0;
  var waitFor = 0;
  var client = this.client;
  var self = this;

  function _finish(err) {
    if (++finished === 3)
      return callback(err);
  }

  function _init(bucket) {
    if (!bucket || !bucket.name || !bucket.props)
      _finish(null);

    return client.setBucket(bucket.name, bucket.props, function(err) {
      return _finish(err);
    });
  }

  _init(this.bucket);
  _init(this.uniqueIndexBucket);
  _init(this.changelogBucket);
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


RiakBackend.prototype.search = function(handlers) {
  return this._operation(search, handlers);
};


RiakBackend.prototype.changelogSearch = function(handlers) {
  var self = this;
  return this._operation(search, handlers, function() {
    return function setup(req, res, next) {
      req.riak = {
        bucket: self.changelogBucket.name,
        client: self.client,
        indexes: self.changelogBucket.indexes,
        key: req.dn.toString(),
        log: self.log4js.getLogger('Riak' + req.type),
        uniqueIndexBucket: uuid(),
        uniqueIndexes: []
      };

      return next();
    };
  });
};

RiakBackend.prototype._operation = function(op, handlers, setup) {
  if (!handlers)
    handlers = [];

  if (!Array.isArray(handlers))
    handlers = [handlers];
  handlers.unshift(setup ? setup() : this._setup());
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
      changelogBucket: self.changelogBucket ? self.changelogBucket.name : false,
      uniqueIndexes: self.uniqueIndexes,
      changelog: self.changelogBucket
    };

    return next();
  };
};


RiakBackend.prototype.toString = function() {
  var self = this;
  return this.name + ': ' + JSON.stringify({
    url: self.client.url,
    bucket: self.bucket,
    uniqueIndexBucket: self.uniqueIndexBucket,
    changelogBucket: self.changelogBucket,
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
