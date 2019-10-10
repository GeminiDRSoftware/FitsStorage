#!/bin/bash

echo Hello, World

echo Checking for docker containers
docker container ps -a

pip install requirements-test.txt || exit 1

echo done
