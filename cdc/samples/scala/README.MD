Get Started
===========
[![Build Status](https://travis-ci.com/flixdb/flixdb.svg?branch=master)](https://travis-ci.com/flixdb/flixdb)
[![Gitter chat](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/flixdb/community "Gitter chat")

Steps:

Start a PostgreSQL container:
```
docker run -p5432:5432 -it flixdb-docker-images.bintray.io/flixdb/postgresql:12.2 
```

The containers at `flixdb-docker-images.bintray.io` have the necessary PostgreSQL configuration (i.e. `wal_level` set to `logical`).

Log in using a PostgreSQL GUI client. The user/password combination is
`docker` / `docker` and there's an empty database called `docker`. 

Run one of the samples from sbt!

Create some tables and see how the changes are captured !
 