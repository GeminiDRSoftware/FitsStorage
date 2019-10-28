#!/bin/bash
BASEDIR=$(dirname "$0")
docker image build -t fitsimage:latest "$BASEDIR/../archive"
