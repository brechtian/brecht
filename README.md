## FlixDb
[![Build Status](https://travis-ci.com/flixdb/flixdb.svg?branch=master)](https://travis-ci.com/flixdb/flixdb)
[![Gitter chat](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/flixdb/community "Gitter chat")


FlixDb is an event store for DDD/CQRS/ES systems. Backed up by PostgreSQL, it seamlessly integrates with Apache Kafka.

## Current State
Very alpha. 

To run "everything" (Kafka, PostgreSQL, Grafana, Prometheus and Brecht itself) in one container:

```
docker run -p8080:8080 -p9092:9092 -p3000:3000 -p5432:5432 -p9090:9090 -p9091:9091 -d brecht:0.1
```

## Commercial Support
FlixDb is sponsored by consulting services. The author(s) provide services centered around 
event driven systems. Send a direct email to flixdb@protonmail.com.
 
## Contributing

### Code Contributors

We welcome contributions to FlixDb and would love for you to help build FlixDb.
