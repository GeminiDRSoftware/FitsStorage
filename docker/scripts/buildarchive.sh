#!/bin/bash
BASEDIR=$(dirname "$0")
pushd "$BASEDIR/../.."
docker image build -t fitsimage:latest -f docker/archive//Dockerfile .
popd
