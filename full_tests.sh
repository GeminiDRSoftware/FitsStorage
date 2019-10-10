#!/bin/bash

echo Hello, World

echo Checking for docker containers
docker container ps -a

which python
python --version
which pip

pip install requirements-test.txt || exit 1

echo done
