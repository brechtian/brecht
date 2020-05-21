#!/bin/sh
set -e
docker login -u sebastianharko -p $BINTRAY_API_KEY brechtian-docker-images.bintray.io

docker push brechtian-docker-images.bintray.io/brecht/postgresql:9.4
docker push brechtian-docker-images.bintray.io/brecht/postgresql:10.12
docker push brechtian-docker-images.bintray.io/brecht/postgresql:11.7
docker push brechtian-docker-images.bintray.io/brecht/postgresql:12.2

docker tag brecht:0.1 brechtian-docker-images.bintray.io/brecht/brecht:0.1
docker push brechtian-docker-images.bintray.io/brecht/brecht:0.1