A fully backend for [ldapjs](http://ldapjs.org) built over [Riak](http://wiki.basho.com).

## Usage

    var ldap = require('ldapjs');
    var ldapRiak = require('ldapjs-riak');

    var SUFFIX = 'o=example';

    var server = ldap.createServer();
    var backend = ldapRiak.createBackend({
      "host": "localhost",
      "port": 8098,
      "bucket": "o_example",
      "indexes": ["l", "cn"],
      "uniqueIndexes": ["uid"],
      "numConnections": 5
    });

    server.add(SUFFIX, backend, backend.add());
    server.modify(SUFFIX, backend, backend.modify());
    server.bind(SUFFIX, backend, backend.bind());
    server.compare(SUFFIX, backend, backend.compare());
    server.del(SUFFIX, backend, backend.del());
    server.search(SUFFIX, backend, backend.search(searchSalt));

    server.listen(config.port, config.host, function() {
      console.log('ldap-riak listening at: %s', server.url);
    });

More docs to follow...

## Installation

    npm install ldapjs-riak

## License

MIT.

## Bugs

See <https://github.com/mcavage/node-ldapjs-riak/issues>.
