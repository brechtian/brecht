#!/bin/bash
echo "Removing postgres104 test container if running..."
docker stop mypg 
docker rm mypg
echo "Starting postgres104 container..."
docker run -d -p5432:5432 --name mypg sebastianharko/postgres104:latest
echo "Running tests..."
sbt core/test
echo "Removing postgres104 test container ..."
docker stop mypg
docker rm mypg

