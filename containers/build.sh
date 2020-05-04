#!/bin/sh
set -e
docker build -t postgresql:9.4 9.4/.
docker build -t postgresql:10.12 10.12/.
docker build -t postgresql:11.7 11.7/.
docker build -t postgresql:12.2 12.2/.
