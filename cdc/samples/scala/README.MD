Get Started
===========

[![Build Status](https://travis-ci.com/brechtian/brechtian.svg?branch=master)](https://travis-ci.com/brechtian/brecht)
[![Gitter chat](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/brechtian/community "Gitter chat")

Steps:

Clone this repo.

```
cd brecht/cdc/samples/scala
sbt compile
```

Start a PostgreSQL container:
```
docker run -p5432:5432 -it brechtian-docker-images.bintray.io/brecht/postgresql:12.2 
```

The containers at `brechtian-docker-images.bintray.io` have the necessary PostgreSQL configuration (i.e. `wal_level` set to `logical`)
and also contain the `wal2jon` plugin.

Log in using a PostgreSQL GUI client. The user/password combination is
`docker` / `docker` and there's an empty database called `docker`. 

Run one of the samples from sbt!

Create some tables and see how the changes are captured !
 