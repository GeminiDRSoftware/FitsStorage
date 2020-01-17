#!/bin/bash
BASEDIR=$(dirname "$0")
UPLOAD=$1
pushd "$BASEDIR/../.."
docker image build -t fitsstorageutils:latest -f docker/fitsstorage/Dockerfile .

BRANCH=`git rev-parse --abbrev-ref HEAD`
echo Branch is $BRANCH
if [[ $UPLOAD == "-u" ]]
then
  if [[ "$BRANCH" == "master" ]]
  then
    echo "setting label to latest"
    docker login gitlab.gemini.edu:4567
    docker tag fitsstorageutils:latest gitlab.gemini.edu:4567/drsoftware/fitsstorage/fitsstorageutils:latest
    docker push gitlab.gemini.edu:4567/drsoftware/fitsstorage/fitsstorageutils:latest
  elif [[ "$BRANCH" == "2020-1" ]]
  then
    echo "setting label to 2020-1"
  fi
fi

popd
