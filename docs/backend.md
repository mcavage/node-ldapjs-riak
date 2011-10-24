---
title: Backend | ldapjs-riak
markdown2extras: wiki-tables
logo-color: green
logo-font-family: google:Aldrich, Verdana, sans-serif
header-font-family: google:Aldrich, Verdana, sans-serif
---

# Backend Configuration and Tuning

This document describes how to create an use a ldapjs-riak backend,
how it works, tuning, and setting up Riak.

# Riak Backend Overview

The ldapjs-riak package stores all data in Riak, and uses Riak's 2i
feature (present in the 1.0+ version of Riak) to support fast querying
at search time.  There is no additional dependency on other
database/caching components (like Redis).  However, the backend is
designed around (relatively) infrequent writes, with frequent reads,
and specifically reads where you know you're going to be searching
against an indexed attribute.  Non-indexed queries are basically going
to be really bad at small scale, and not work at all at large scale.

The backend supports "normal" indexing, which means that you can have
multiple entries in the directory with the same attribute/value
pairs. In addition unique indexing is supported, but to do so, the
backend maintains a separate bucket in Riak to keep track of seen
attribute/value pairs (i.e., unique indexes are maintained
"manually").  Note this means that in failure modes it is possible to
write an entry while failing to write unique index records. This is
why it's important to tune retry/backoff setting appropriately.

Also, the backend can optionally be configure to write LDAP changelog
records on all updates. The changelog records are _almost_ compliant
with the [http://tools.ietf.org/html/draft-good-ldap-changelog-04](LDAP
Changelog RFC Draft), but differ in that (1) changes are written as
JSON, not LDIF, and (2) DNs are up to you to sequence/define.
ldapjs-riak changelog records are written to yet another bucket, and
notably are written *after* responding to the client, so it is
possible for the client to see `LDAP_SUCCESS` but the changelog
recording action to fail.

It's pretty straight-forward to think about how this would work, but
here's a quick break down of the work done by each operation:

- *add(dn, entry):*
    1. Check if `dn` exists
    2.  Check if the parent of `dn` exists
    3.  Add _operational_ attributes (like ctime/mtime/etc.).
    4.  Generate list of unique indexes, and ensure they are indeed
    unique
    5.  Save the entry
    6.  Save the unique indexes
    7.  (optional) Write a changelog record
- *bind(dn, credentials):*
    1. Lookup entry
    2.  Check credentials
- *compare(dn, attr, val):*
    1. Lookup entry
    2.  Compare attribute/value
- *delete(dn):*
    1. Load entry
    2. Check if children exist
    3. Delete the main record
    4. Delete any unique indexes
    5. (optional) Write a changelog record.
- *modifyDN(dn, newDN):*
    1. Load entry
    2. Check if children exist
    3. Check if new parent exists
    4. Delete existing record
    5. Delete unique indexes
    6. Save new record
    7. Resave unique indexes
    8. (optional) Write a changelog record
- *modify(dn, changes):*
    1. Load entry
    2. Make changes
    3. Check uniqueness of changes
    4. Delete old unique indexes
    5. Save entry
    6. Save new unique indexes
    7. (optional) Write a changelog record
- *search(baseDN, scope, filter):*
    1. If scope=base, just resolve as a Riak GET
    2. Otherwise, introspect the filter, and try to use an indexed
    attribute
    3. As keys come in, load records, and check against the search
    filter to send back

Note that the search operation will _not_ return results sorted by DN;
results are streamed back as we get them from Riak. This is different
than most every other LDAP server out there, but is fine for most
cases, as you get data faster. Sort client-side if you need to do so.

# Setup and Creation

## Configure Riak to use leveldb

Obviously, to leverage Riak, you need to install Riak.  Grab a 1.0.x
release from [Basho](http://basho.com), and follow their setup
instructions.  Post-install, you'll need to edit Riak's `app.config`
`storage_backend` setting to:

    {storage_backend, riak_kv_eleveldb_backend},

The default will have been `bitcask`.  ldapjs-riak basically doesn't
work, at all, without Riak's 2i feature, so this is required.

Other than that, do whatever you would do with Riak to setup a
cluster, tune memory setttings, add a load balancer, etc.  It's out of
scope for this document to tell you how to deploy Riak to production...

## Determine how to configure the backend

The Riak backend has the following configurations:

* Cluster information
* CAP tuning
* Indexes/Unique Indexes
* Changelog

### Riak Cluster

You configure the backend to point at a single IP/port combination, so
really you should setup a load balancer in front of your Riak cluster,
or do IP-takeovers, or something.  But you also configure retry/backoff
settings, which uses [node-retry](https://github.com/tim-kos/node-retry); note
that these retry settings kick in on *every* request to Riak, so you
probably want to keep this bounded, as a single add for example
will hit Riak at minimum for the save, plus once for each unique
index. Modify/Delete/ModifyDN are worse.


        "client": {
          "url": "http://localhost:8098",
          "clientId": "my-laptop",
          "retry": {
            "retries": 3,
            "factor": 2,
            "minTimeout": 1000,
            "maxTimeout": 10000
          }
        }

And `clientId` is the Riak identifier for this client. Just make
something up.

### CAP Tuning

As Riak nicely allows you to tune the replication/consistency/availability
settings for each bucket, this backend allows you to tune the CAP
settings for all three buckets (data, unique indexing, and
changelog).

The recommended tuning is to use the default "quorum" on the data
bucket, use strong consistency on the unique index bucket (this means
that in the event of a partition you won't be able to take writes),
and do whatever you want on changelog (probably quorum makes sense).

# Create a Backend

If you're not familiar wth [ldapjs](http://ldapjs.org), get familiar,
as the rest of this won't make any sense otherwise.  ldapjs includes
the ability to keep a "backend" object that is stateful, and this
module leverages that functionality.  The bare minimum you need to get
going is the following:

    var ldapRiak = require('ldapjs-riak');
    var backend = ldapRiak.createBackend({
        "bucket": {
          "name": "ldapjs_riak",
        },
        "uniqueIndexBucket": {
          "name": ldapjs_riak_uindex",
        },
        "client": {
          "url": "http://localhost:8098",
          "clientId": "ldapjs_riak"
        }
    });

Which will create a backend, and point it at the specified Riak
host/port/buckets, with no indexes.  Once you have that, you can mount
the backend "as normal" in ldapjs:

    var ldap = require('ldapjs');

    var SUFFIX = 'dc=example, dc=com';

    var server = ldap.createServer({});

    server.add(SUFFIX, backend, backend.add());
    server.modify(SUFFIX, backend, backend.modify());
    server.bind(SUFFIX, backend, backend.bind());
    server.compare(SUFFIX, backend, backend.compare());
    server.del(SUFFIX, backend, backend.del());
    server.modifyDN(SUFFIX, backend, backend.modifyDN());
    server.search(SUFFIX, backend, backend.search());

While that's kind of annoyingly verbose, each of the operations takes
the ability to inject handlers that run after backend
intiialization has been run, but before "real work" gets kicked
off. So for example:

    server.compare(SUFFIX, backend, function(req, res, next) {
      return next();
    }, backend.compare(function(req, res, next) {
      req.riak.log('hello world');
    }));

While that does nothing interesting, it does show that you can still
use "normal" handlers with ldapjs, as well as special "ldapjs-riak" handlers.

## createBackend(options)

The full list of options (options is a plain JS object) to `createBackend` is:

||bucket||Object||required||A configuration of the Riak bucket name and CAP tunings for entries. ||
||log4js||Log4JS Instance||required||`require('log4js')` or other configured instance.||
||client||Object||required||Connection information for the actual Riak cluster.||
||uniqueIndexBucket||Object||optional||A configuration of the Riak bucket name and CAP tunings for unique indexes.||
||changelogBucket||Object||optional||A configuration of the Riak bucket name and CAP tunings for changelogging.||
||indexes||Object||optional||A listing of attributes to index in an entry, and whether or not uniquness should be enforced.||

