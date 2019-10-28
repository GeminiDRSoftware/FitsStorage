#!/bin/bash
BASEDIR=$(dirname "$0")
pushd "$BASEDIR/../.."
docker image build -t fitsstorageutils:latest -f docker/fitsstorage/Dockerfile .
popd
