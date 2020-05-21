#!/bin/sh
set -e
docker login -u sebastianharko -p $BINTRAY_API_KEY brechtian-docker-images.bintray.io
docker tag postgresql:9.4 brechtian-docker-images.bintray.io/brecht/postgresql:9.4
docker tag postgresql:10.12 brechtian-docker-images.bintray.io/brecht/postgresql:10.12
docker tag postgresql:11.7 brechtian-docker-images.bintray.io/brecht/postgresql:11.7
docker tag postgresql:12.2 brechtian-docker-images.bintray.io/brecht/postgresql:12.2
docker push brechtian-docker-images.bintray.io/brecht/postgresql:9.4
docker push brechtian-docker-images.bintray.io/brecht/postgresql:10.12
docker push brechtian-docker-images.bintray.io/brecht/postgresql:11.7
docker push brechtian-docker-images.bintray.io/brecht/postgresql:12.2
