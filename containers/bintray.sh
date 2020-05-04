#!/bin/sh
set -e
docker login -u sebastianharko -p $BINTRAY_API_KEY flixdb-docker-images.bintray.io
docker tag postgresql:9.4 flixdb-docker-images.bintray.io/flixdb/postgresql:9.4
docker tag postgresql:10.12 flixdb-docker-images.bintray.io/flixdb/postgresql:10.12
docker tag postgresql:11.7 flixdb-docker-images.bintray.io/flixdb/postgresql:11.7
docker tag postgresql:12.2 flixdb-docker-images.bintray.io/flixdb/postgresql:12.2
docker push flixdb-docker-images.bintray.io/flixdb/postgresql:9.4
docker push flixdb-docker-images.bintray.io/flixdb/postgresql:10.12
docker push flixdb-docker-images.bintray.io/flixdb/postgresql:11.7
docker push flixdb-docker-images.bintray.io/flixdb/postgresql:12.2
