#!/bin/sh
set -e
docker build -t postgresql:9.4 postgresql/9.4/.
docker build -t postgresql:10.12 postgresql/10.12/.
docker build -t postgresql:11.7 postgresql/11.7/.
docker build -t postgresql:12.2 postgresql/12.2/.
