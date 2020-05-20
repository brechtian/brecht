#!/bin/sh
set -e
cp ../core/target/universal/core-0.1.zip brecht/core-0.1.zip
docker build -t brecht:0.1 brecht/.
rm -rf brecht/core-0.1.zip