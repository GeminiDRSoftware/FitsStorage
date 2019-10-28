#!/bin/bash
BASEDIR=$(dirname "$0")
docker image build -t fitsstorageutils:latest "$BASEDIR/../fitsstorage"
