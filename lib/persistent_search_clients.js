///--- API


// Cache used to store connected persistent search clients
function PersistentSearchClients() {
  this.clientList = [];
}
module.exports = PersistentSearchClients;


PersistentSearchClients.prototype.addClient = function(req, res, callback) {
  if (typeof(req) !== 'object')
    throw new TypeError('req must be an object');
  if (typeof(res) !== 'object')
    throw new TypeError('res must be an object');
  if (callback && typeof(callback) !== 'function')
    throw new TypeError('callback must be a function');

  var log = req.log;

  var client = {};
  client.req = req;
  client.res = res;

  log.debug('storing client');

  this.clientList.push(client);

  log.debug('stored client');
  log.debug('total number of clients', this.clientList.length);
  if (callback)
    callback(client);
};


PersistentSearchClients.prototype.removeClient = function(req, res, callback) {
  if (typeof(req) !== 'object')
    throw new TypeError('req must be an object');
  if (typeof(res) !== 'object')
    throw new TypeError('res must be an object');
  if (callback && typeof(callback) !== 'function')
    throw new TypeError('callback must be a function');

  var log = req.log;
  log.debug('removing client');
  var client = {};
  client.req = req;
  client.res = res;

  // remove the client if it exists
  this.clientList.forEach(function(element, index, array) {
    if (element.req === client.req) {
      log.debug('removing client from list');
      array.splice(index, 1);
    }
  });

  log.debug(this.clientList.length);
  if (callback)
    callback(client);
};
