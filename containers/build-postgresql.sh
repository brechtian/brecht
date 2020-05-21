#!/bin/sh
set -e
docker build -t postgresql:9.4 postgresql/9.4/.
docker build -t postgresql:10.12 postgresql/10.12/.
docker build -t postgresql:11.7 postgresql/11.7/.
docker build -t postgresql:12.2 postgresql/12.2/.
docker tag postgresql:9.4 brechtian-docker-images.bintray.io/brecht/postgresql:9.4
docker tag postgresql:10.12 brechtian-docker-images.bintray.io/brecht/postgresql:10.12
docker tag postgresql:11.7 brechtian-docker-images.bintray.io/brecht/postgresql:11.7
docker tag postgresql:12.2 brechtian-docker-images.bintray.io/brecht/postgresql:12.2
