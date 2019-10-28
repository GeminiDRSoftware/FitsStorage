#!/bin/bash
BASEDIR=$(dirname "$0")
docker image build -t fitsimage:centos8 "$BASEDIR/../archive-centos8/"
