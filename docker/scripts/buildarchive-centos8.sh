#!/bin/bash
BASEDIR=$(dirname "$0")
pushd "$BASEDIR/../.."
docker image build -t fitsimage:centos8 -f docker/archive-centos8//Dockerfile .
popd
