#!/bin/bash
BASEDIR=$(dirname "$0")
docker image build -t gemini-jenkins:latest "$BASEDIR/../jenkins/"
