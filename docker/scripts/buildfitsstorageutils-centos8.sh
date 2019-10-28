#!/bin/bash
BASEDIR=$(dirname "$0")
pushd "$BASEDIR/../.."
docker image build -t fitsstorageutils:centos8 -f docker/fitsstorage-centos8/Dockerfile .
popd
