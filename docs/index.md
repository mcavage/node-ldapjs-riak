---
title: ldapjs-riak
markdown2extras: wiki-tables
logo-color: green
logo-font-family: google:Aldrich, Verdana, sans-serif
header-font-family: google:Aldrich, Verdana, sans-serif
---

<div id="indextagline">
High-Availability <a href="http://tools.ietf.org/html/rfc4510"
id="indextaglink">LDAP</a> using <a id="indextaglink"
href="http://ldapjs.org">ldapjs</a> and <a id="indextaglink"
href="http://basho.com">Riak</a>
</div>

# Overview

ldapjs-riak is a full LDAP backend implementation for
[ldapjs](http://ldapjs.org) and [Riak](http://basho.com).
Using ldapjs-riak, you can easily stand up a highly available LDAP
cluster. To get bootstrapped, the following code will give you a
v3-compliant LDAP server that allows a user to make changes to their
own entry, and any child entries (as an example):

    var ldap = require('ldapjs');
    var ldapRiak = require('ldapjs-riak');

    var SUFFIX = 'o=example';

    function authorize(req, res, next) {
      var bindDN = req.connection.ldap.bindDN;

      if (req.type === 'BindRequest' ||
          bindDN.parentOf(req.dn) ||
          bindDN.equals(req.dn))
        return next();

      return next(new ldap.InsufficientAccessRightsError());
    }


    var server = ldap.createServer();
    var backend = ldapRiak.createBackend({
        "bucket": {
          "name": "ldapjs_riak",
        },
        "uniqueIndexBucket": {
          "name": ldapjs_ldapjs_riak",
        },
        "indexes": {
          "email": true,
          "uuid": true,
          "cn": false,
          "sn": false
        },
        "client": {
          "url": "http://localhost:8098",
          "clientId": "ldapjs_riak_1",
          "retry": {
            "retries": 3,
            "factor": 2,
            "minTimeout": 1000,
            "maxTimeout": 10000
          }
        }
    });

    server.bind('cn=root', function(req, res, next) {
      if (req.version !== 3)
        return next(new ldap.ProtocolError(req.version + ' is not v3'));

      if (req.credentials !== 'secret')
        return next(new ldap.InvalidCredentialsError(req.dn.toString()));

      res.end();
      return next();
    });

    server.add(SUFFIX, backend, authorize, backend.add());
    server.modify(SUFFIX, backend, authorize, backend.modify());
    server.bind(SUFFIX, backend, authorize, backend.bind());
    server.compare(SUFFIX, backend, authorize, backend.compare());
    server.del(SUFFIX, backend, authorize, backend.del());
    server.modifyDN(SUFFIX, backend, authorize, backend.modifyDN());
    server.search(SUFFIX, backend, authorize, backend.search());

    server.listen(1389, function() {
      console.log('ldap-riak listening at: %s', server.url);
    });

Note that ldapjs-riak requires Riak 1.0 with the `eleveldb_backend`,
as it makes heavy use of Riak's secondary indexing feature.  Once you
have a Riak instance running, and are running that code, try:

    $ ldapadd -x -D cn=root -w secret -H ldap://localhost:1389 -f data.ldif

Where data.ldif has:

    dn: o=example
    o: example
    objectclass: organization

    dn: email=nobody@acme.com, o=example
    cn: Sample
    sn: User
    email: nobody@acme.com
    userpassword: secret2
    objectclass: person

Now you can try searching as the child user:

    $ ldapsearch -x -D email=nobody@acme.com,o=example -w secret2 -H ldap://localhost:1389 -b email=nobody@acme.com,o=example email=*

All of the standard LDAP operations:

- Add
- Bind
- Compare
- Delete
- Modify
- ModifyDN
- Search

are supported by the Riak backend.  In addition, it supports an
"almost" compliant changelog implementation (there are a few
differences, like instead of storing changes in LDIF, they are stored
in JSON).

# Why shouldn't you use ldapjs-riak?

Because it (and Riak itself) are not a perfect fit for all use cases.
That's true of any technology.  Specifically, what this is aimed at is
a "big data" use case, where the workload breakdown is skewed to be
very read-heavy, and you can plan the queries you'll need (mostly) in
advance. Because ldapjs-riak leverages Riak's 2i feature heavily, if
you're planning to do a lot of ad-hoc type information finding (i.e.,
on non-indexed data), you're going to be off the reservation.

# What else do I need to do?

The sample code above will actually get you a fully-functional LDAP
server. That said, you probably want to look at writing some of your
own code for:

- *Admin Users/Authentication:* You probably want a 'root' user
   configured that's not stored in Riak, so you can authenticate when
   Riak is unavailable.
- *Authorization:* In the example above I just assert that a DN can do
   anything to entries below it, or at it.  It would not, for example
   stop a modifyDN from happening, and putting itself somewhere else in
   the tree. You probably want something richer here.
- *Auditing:*  I just write out some "w3c style" logs that I can post-process.
- *Schema:* While I sort of loathe standard LDAP schema, it does serve
   a purpose.  I just use a simple "validations" framework that's sort
   of similar to Rails/Django modeling, but simpler.
- *Password salting:* Yeah, this is pretty important if you're storing
   credentials.  You'll need to intercept each request you care about
   to make it behave correctly.
- *Extra indexing/transforms:* If you wanted to store a "parent"
   attribute, for example, that was filled in server-side.  Basically,
   anything you'd use an SQL trigger for.

# Installing

    $ npm install ldapjs-riak

`Nuff said.

# More information

||[backend](/backend.html)||Reference for creating and configuring the backend.||

||License||[MIT](http://opensource.org/licenses/mit-license.php)||
||Code||[mcavage/node-ldapjs-riak](https://github.com/mcavage/node-ldapjs-riak)||
||node.js version||0.4.x and 0.5.x||
||Twitter||[@mcavage](http://twitter.com/mcavage)||
