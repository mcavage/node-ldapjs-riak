// Copyright 2011 Mark Cavage, Inc.  All rights reserved.

var assert = require('assert');
var http = require('http');
var https = require('https');
var url = require('url');
var util = require('util');

var qs = require('querystring');
var retry = require('retry');
var uuid = require('node-uuid');
var sprintf = require('sprintf').sprintf;



///--- Internal Helpers

function httpDate(date) {
  function pad(val) {
    if (parseInt(val, 10) < 10) {
      val = '0' + val;
    }
    return val;
  }

  if (!date)
    date = new Date();

  var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  var days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  return days[date.getUTCDay()] + ', ' +
    pad(date.getUTCDate()) + ' ' +
    months[date.getUTCMonth()] + ' ' +
    date.getUTCFullYear() + ' ' +
    pad(date.getUTCHours()) + ':' +
    pad(date.getUTCMinutes()) + ':' +
    pad(date.getUTCSeconds()) +
    ' GMT';
}


function RiakError(res, message) {
  if (Error.captureStackTrace)
    Error.captureStackTrace(this, arguments.callee);

  var msg = http.STATUS_CODES[res.statusCode] || 'Unknown';
  Error.call(this, msg);
  this.__defineGetter__('code', function() {
    return res.statusCode;
  });
  this.__defineGetter__('headers', function() {
    return res.headers;
  });
  this.__defineGetter__('message', function() {
    return message || msg;
  });
  this.__defineGetter__('name', function() {
    return msg.replace(/\s/, '') + 'Error';
  });
}
util.inherits(RiakError, Error);



///--- API

/**
 * Creates a new (single) Riak client via HTTP.
 *
 * Defaults (options):
 *  - url: http://localhost:8098
 *  - log4js: require('log4js')
 *  - retry: {
 *      retries: 3,
 *      factor: 2,
 *      minTimeout: 1s,
 *      maxTimeout: 60s
 *    }
 *  - clientId: uuid()
 *  - headers: {}
 *
 * @param {Object} options see above.
 */
function Riak(options) {
  if (!options)
    options = {};
  if (typeof(options) !== 'object')
    throw new TypeError('options (object) required');

  if (!options.headers)
    options.headers = {};
  if (!options.url)
    options.url = 'http://localhost:8098';
  if (!options.clientId)
    options.clientId = uuid();
  if (!options.log4js)
    options.log4js = require('log4js');
  if (!options.retry)
    options.retry = {
      retries: 3,
      factor: 2,
      minTimeout: 1 * 1000,
      maxTimeout: 60 * 1000
    };

  var self = this;
  var _url = url.parse(options.url);
  this.__defineGetter__('url', function() {
    return _url;
  });
  this.__defineGetter__('id', function() {
    return options.clientId;
  });
  this.__defineGetter__('log', function() {
    if (!self._log)
      self._log = options.log4js.getLogger('Riak');

    return self._log;
  });
  this.__defineGetter__('retry', function() {
    return retry.operation(options.retry);
  });
}
module.exports = Riak;


/**
 * Performs a Riak ListBuckets operation.
 *
 * @param {Function} callback of the form f(err, buckets).
 */
Riak.prototype.listBuckets = function(callback) {
  if (typeof(callback) !== 'function')
    throw new TypeError('callback (function) required');

  var opts = {
    path: '/riak?buckets=true'
  };
  this._request(opts, function(err, obj, res) {
    if (err)
      return callback(err);

    return callback(null, obj.buckets || [], res.headers);
  });
};


/**
 * Performs a Riak ListKeys operation.
 *
 * @param {String} bucket bucket name.
 * @param {Function} callback of the form f(err, keys, headers).
 */
Riak.prototype.listKeys = function(bucket, callback) {
  if (!bucket || typeof(bucket) !== 'string')
    throw new TypeError('bucket (string) required');
  if (typeof(callback) !== 'function')
    throw new TypeError('callback (function) required');

  var opts = {
    path: sprintf('/riak/%s?keys=true&props=false', qs.escape(bucket))
  };
  this._request(opts, function(err, obj, res) {
    if (err)
      return callback(err);

    return callback(null, obj.keys || []), res.headers;
  });
};


/**
 * Performs a Riak GetBucket operation.
 *
 * Does not list keys; use ListKeys for that.
 *
 * @param {String} bucket bucket name.
 * @param {Function} callback of the form f(err, properties).
 */
Riak.prototype.getBucket = function(bucket, callback) {
  if (!bucket || typeof(bucket) !== 'string')
    throw new TypeError('bucket (string) required');
  if (typeof(callback) !== 'function')
    throw new TypeError('callback (function) required');

  var opts = {
    path: sprintf('/riak/%s', qs.escape(bucket))
  };
  this._request(opts, function(err, obj, res) {
    if (err)
      return callback(err);

    return callback(null, obj.props || {}, res.headers);
  });
};


/**
 * Performs a Riak SetBucket operation.
 *
 * Options takes params exactly as specified in the basho wiki, without the
 * 'props' key.
 *
 * @param {String} bucket bucket name.
 * @param {Object} options properties to write.
 * @param {Function} callback of the form f(err, properties).
 */
Riak.prototype.setBucket = function(bucket, options, callback) {
  if (!bucket || typeof(bucket) !== 'string')
    throw new TypeError('bucket (string) required');
  if (!options || typeof(options) !== 'object')
    throw new TypeError('options (object) required');
  if (typeof(callback) !== 'function')
    throw new TypeError('callback (function) required');

  var opts = {
    path: sprintf('/riak/%s', qs.escape(bucket)),
    method: 'PUT'
  };
  this._request(opts, function(err, obj, res) {
    if (err)
      return callback(err);

    return callback((res.statusCode !== 204 ? new RiakError(res) : null),
                    res.headers);
  }, function() {
    return JSON.stringify({ props: options });
  });
};


/**
 * Performs a Riak FetchObject.
 *
 * @param {String} bucket bucket name.
 * @param {String} key key name.
 * @param {Object} options optional properties (r, vtag, and 'headers').
 * @param {Function} callback of the form f(err, obj, properties).
 * @param {Boolean} head optionally set this to true to perform a HEAD.
 */
Riak.prototype.fetchObject = function(bucket, key, options, callback, head) {
  if (!bucket || typeof(bucket) !== 'string')
    throw new TypeError('bucket (string) required');
  if (!key || typeof(key) !== 'string')
    throw new TypeError('key (string) required');
  if (typeof(options) === 'function') {
    callback = options;
    options = {};
  }
  if (typeof(options) !== 'object')
    throw new TypeError('options must be an object');
  if (typeof(callback) !== 'function')
    throw new TypeError('callback (function) required');

  var opts = {
    path: sprintf('/riak/%s/%s', qs.escape(bucket), qs.escape(key)),
    headers: options.headers,
    method: head ? 'HEAD' : 'GET'
  };
  this._request(opts, function(err, obj, res) {
    if (err)
      return callback(err);

    if (res.statusCode === 300)
      return callback(new RiakError(res, 'Multiple Choices'));
    if (res.statusCode === 404)
      return callback(new RiakError(res, opts.path + ' not found'));
    if (res.statusCode !== 200)
      return callback(new RiakError(res));

    return callback(null, obj || {}, res.headers);
  });
};


/**
 * Performs a Riak StoreObject.
 *
 * @param {String} bucket bucket name.
 * @param {String} key key name.
 * @param {Object} object JSON object to store.
 * @param {Object} options properties (r, vtag, 'indexes' and 'headers').
 * @param {Function} callback of the form f(err, key, properties).
 */
Riak.prototype.storeObject = function(bucket, key, object, options, callback) {
  if (!bucket || typeof(bucket) !== 'string')
    throw new TypeError('bucket (string) required');
  switch (typeof(key)) {
  case 'string':
    break;
  case 'object':
    if (typeof(object) === 'function') {
      callback = object;
      object = key;
      options = {};
      key = '';
    } else if (typeof(options) === 'function') {
      callback = options;
      options = object;
      object = key;
      key = '';
    } else {
      throw new TypeError('key must be a string');
    }
    break;
  default:
    throw new TypeError('key must be a string');
  }
  if (typeof(object) !== 'object')
    throw new TypeError('object (object) required');
  if (typeof(options) === 'function') {
    callback = options;
    options = {};
  }
  if (typeof(options) !== 'object')
    throw new TypeError('options must be an object');
  if (typeof(callback) !== 'function')
    throw new TypeError('callback (function) required');

  var path;
  if (key)
    path = sprintf('/riak/%s/%s', qs.escape(bucket), qs.escape(key));

  if (!path)
    path = sprintf('/riak/%s', qs.escape(bucket));

  var _query = { returnbody: false };
  if (options.w) _query.w = options.w;
  if (options.dw) _query.dw = options.dw;
  path += '?' + qs.stringify(_query);

  var opts = {
    path: path,
    headers: options.headers || {},
    method: key ? 'PUT' : 'POST'
  };

  // Add indexes
  if (options.indexes) {
    var _indexes = this.indexObject(options.indexes, object);
    Object.keys(_indexes).forEach(function(i) {
      opts.headers[i] = _indexes[i];
    });
  }

  return this._request(opts, function(err, obj, res) {
    if (err)
      return callback(err);

    if (res.statusCode === 201)
      return callback(null, res.headers.location.split('/').pop(), res.headers);

    if (res.statusCode === 300)
      return callback(new RiakError(res, 'Multiple Choices'));

    if (res.statusCode !== 200 && res.statusCode !== 204)
      return callback(new RiakError(res));

    return callback(null, key, res.headers);
  }, function() {
    return JSON.stringify(object);
  });
};


/**
 * Generates an object of HTTP headers that direct Riak to index when you
 * save a key.
 *
 * @param {Array} indexes list of fields to index (strings).
 * @param {Object} object the target object you want Riak to index.
 */
Riak.prototype.indexObject = function(indexes, object) {
  if (!indexes || (typeof(indexes) !== 'string' && !Array.isArray(indexes)))
    throw new TypeError('indexes ([string]) required');
  if (!object || typeof(object) !== 'object')
    throw new TypeError('object (object) required');

  if (!Array.isArray(indexes))
    indexes = [indexes];

  var headers = {};

  function _header(index, value) {
    if (!/\w_(bin|int)$/.test(index))
      index = index + '_bin';

    headers['x-riak-index-' + index] = qs.escape(value);
    return headers;
  }

  function _isIndex(key) {
    for (var i = 0; i < indexes.length; i++)
      if (indexes[i].replace(/_(bin|int)$/, '') === key)
        return indexes[i];

    return false;
  }

  function _index(key, value) {
    var i;
    switch (typeof(value)) {
    case 'string':
    case 'boolean':
    case 'number':
      i = _isIndex(key);
      if (i)
        _header(i, value + '');
      break;
    case 'object':
      if (value === null)
        return;
      if (Array.isArray(value)) {
        value.forEach(function(v) {
          return _index(key, v);
        });
      } else {
        Object.keys(value).forEach(function(k) {
          return _index(k, value[k]);
        });
      }
      break;
    default:
      break;
    }
  }

  Object.keys(object).forEach(function(k) {
    return _index(k, object[k]);
  });
  return headers;
};


/**
 * Performs a Riak DeleteObject.
 *
 * @param {String} bucket bucket name.
 * @param {String} key key name.
 * @param {Object} options properties (rw, and 'headers').
 * @param {Function} callback of the form f(err, properties).
 */
Riak.prototype.deleteObject = function(bucket, key, options, callback) {
  if (!bucket || typeof(bucket) !== 'string')
    throw new TypeError('bucket (string) required');
  if (!key || typeof(key) !== 'string')
    throw new TypeError('key (string) required');
  if (typeof(options) === 'function') {
    callback = options;
    options = {};
  }
  if (typeof(options) !== 'object')
    throw new TypeError('options must be an object');
  if (typeof(callback) !== 'function')
    throw new TypeError('callback (function) required');

  var opts = {
    path: sprintf('/riak/%s/%s', qs.escape(bucket), qs.escape(key)),
    headers: options.headers,
    method: 'DELETE'
  };
  return this._request(opts, function(err, obj, res) {
    if (err)
      return callback(err);

    if (res.statusCode === 404)
      return callback(new RiakError(res, opts.path + ' not found'));
    if (res.statusCode !== 204)
      return callback(new RiakError(res));

    return callback(null, res.headers);
  });
};


/**
 * Performs a Riak FetchObject (by index).
 *
 * To perform a range query pass in an array to $value. Index types default to
 * `_bin`. If that's not what you want, just explicitly set it, like:
 *
 * client.fetchObjectByIndex(bucket, 'time_int', 1234, 'gte', callback);
 *
 * Note this method "auto resolves" objects.
 *
 * @param {String} bucket bucket name.
 * @param {String} index index name.
 * @param {String} value the value to look up.
 * @param {Function} callback of the form f(err, objects, properties).
 */
Riak.prototype.fetchObjectsByIndex = function(bucket, index, value, callback) {
  if (!bucket || typeof(bucket) !== 'string')
    throw new TypeError('bucket (string) required');
  if (!index || typeof(index) !== 'string')
    throw new TypeError('index (string) required');
  if (!value || (typeof(value) !== 'string' && !Array.isArray(value)))
    throw new TypeError('value (string|array[string]) required');
  if (typeof(callback) !== 'function')
    throw new TypeError('callback (function) required');

  if (!Array.isArray(value))
    value = [value];

  if (!/_(bin|int)$/.test(index))
    index = index + '_bin';

  function _escape(v, doubleEncode) {
    return qs.escape(v);
  }

  var opts = {
    path: sprintf('/buckets/%s/index/%s', _escape(bucket), _escape(index))
  };
  value.forEach(function(v) {
    opts.path += '/' + _escape(v);
  });

  var self = this;
  return this._request(opts, function(err, obj, res) {
    if (err)
      return callback(err);

    if (res.statusCode !== 200)
      return callback(new RiakError(res));


    var keys = obj.keys || [];
    if (!keys.length)
      return callback(null, []);


    var finished = 0;
    var objects = [];
    return keys.forEach(function(k) {
      self.fetchObject(bucket, k, function(err, obj, headers) {
        if (err && finished < keys.length) {
          finished = keys.length + 1;
          return callback(err);
        }

        objects.push(obj);
        if (++finished === keys.length)
          return callback(null, objects);
      });
    });
  });
};


/**
 * Takes exactly what Basho's wiki says map reduce takes.
 *
 * @param {Object} inputs map reduce inputs.
 * @param {Object} query reduce phases, etc.
 * @param {Function} callback of f(err, stuff. headers).
 */
Riak.prototype.mapred = function(inputs, query, callback) {
  if (typeof(inputs) !== 'object')
    throw new TypeError('inputs (object) required');
  if (typeof(query) !== 'object')
    throw new TypeError('query (object) required');
  if (typeof(callback) !== 'function')
    throw new TypeError('callback (function) required');

  var opts = {
    path: '/mapred',
    method: 'POST'
  };
  this._request(opts, function(err, obj, res) {
    if (err)
      return callback(err);

    return callback((res.statusCode !== 200 ?
                     new RiakError(res) : null),
                    obj,
                    res.headers);
  }, function() {
    return JSON.stringify({
      inputs: inputs,
      query: query
    });
  });
};



///--- Friendly wrappers

Riak.prototype.put = function(bucket, key, object, options, callback) {
  if (typeof(key) !== 'string')
    return this.setBucket(bucket, options, callback);

  return this.storeObject(bucket, key, object, options, callback);
};


Riak.prototype.get = function(bucket, key, options, callback) {
  if (typeof(key) !== 'string')
    return this.getBucket(bucket, options, callback);

  return this.fetchObject(bucket, key, options, callback);
};

Riak.prototype.head = function(bucket, key, options, callback) {
  return this.fetchObject(bucket, key, options, callback, true);
};

Riak.prototype.list = function(bucket, callback) {
  if (typeof(bucket) !== 'string')
    return this.listBuckets(callback);

  return this.listKeys(bucket, callback);
};


Riak.prototype.find = Riak.prototype.fetchObjectsByIndex;
Riak.prototype.post = Riak.prototype.storeObject;
Riak.prototype.del = Riak.prototype.deleteObject;


///--- Private methods

Riak.prototype._request = function(options, callback, write) {
  assert.ok(options);
  assert.ok(callback);

  var self = this;
  var content = null;

  var opts = {
    host: self.url.hostname || '127.0.0.1',
    port: self.url.port || 8098,
    method: options.method || 'GET',
    path: (self.pathname || '') + (options.path ? options.path : ''),
    headers: options.headers || {}
  };
  opts.headers.accept = 'application/json';
  opts.headers['X-Riak-ClientId'] = self.id;
  opts.headers.date = httpDate();

  if (write) {
    opts.headers['content-type'] = 'application/json';
    content = write();
  }


  var op = self.retry;
  op.attempt(function(attempt) {
    if (self.log.isTraceEnabled()) {
      var logHeaders = '';
      Object.keys(opts.headers).forEach(function(h) {
        logHeaders += sprintf('\n%s: %s', h, opts.headers[h]);
      });
      self.log.trace('%s %s HTTP/1.1 #attempt=%d%s\n%s\n',
                     opts.method, opts.path, attempt, logHeaders,
                     (content ? content : ''));
    }

    function _error(err) {
      if (op.retry(err))
        return;

      return callback(op.mainError());
    }

    function _callback(res) {
      if (self.log.isTraceEnabled()) {
        var logHeaders = '';
        Object.keys(res.headers).forEach(function(h) {
          logHeaders += sprintf('\n%s: %s', h, res.headers[h]);
        });
        self.log.trace('HTTP/1.1 %d%s', res.statusCode, logHeaders);
      }

      res.on('error', function(err) {
        return _error(res, err.message);
      });

      res.setEncoding('utf8');
      res.body = '';
      if (!options.stream) {
        res.on('data', function(chunk) {
          res.body += chunk;
        });
        res.on('end', function() {
          var _obj = null;

          if (res.statusCode >= 500)
            return _error(new RiakError(res, res.body));

          self.log.trace('response received %s', res.body);
          if (res.body && res.headers['content-type'] === 'application/json') {
            try {
              _obj = JSON.parse(res.body) || {};
            } catch (e) {
              return callback(e);
            }
          }

          return callback(null, _obj, res);
        });
      } else {
        return callback(null, res);
      }
    }

    var req = /^https.+/.test(self.url.proto) ?
      https.request(opts, _callback) :
      http.request(opts, _callback);

    req.on('error', function(err) {
      return _error(err);
    });

    if (write)
      req.write(content);

    req.end();
  });
};
