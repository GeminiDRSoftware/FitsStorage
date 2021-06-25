#!/bin/bash
BASEDIR=$(dirname "$0")
pushd "$BASEDIR/../../.."
docker image build -t gemini-jenkins:latest -f "FitsStorage/docker/jenkins/" .
popd
