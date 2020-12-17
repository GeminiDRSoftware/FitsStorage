#!/bin/bash
BASEDIR=$(dirname "$0")
UPLOAD=$1
pushd "$BASEDIR/../.."

BRANCH=`git rev-parse --abbrev-ref HEAD`
echo Branch is $BRANCH
if [[ "$BRANCH" == "2020-2" ]]
then
  LABEL="2020-1"
else
  LABEL="latest"
fi

docker image build -t fitsstorageutils:$LABEL -f docker/fitsstorage/Dockerfile .

if [[ $UPLOAD == "-u" ]]
then
  if [[ "$BRANCH" == "master" ]]
  then
    echo "setting label to $LABEL"
    docker login gitlab.gemini.edu:4567
    docker tag fitsstorageutils:$LABEL gitlab.gemini.edu:4567/drsoftware/fitsstorage/fitsstorageutils:$LABEL
    docker push gitlab.gemini.edu:4567/drsoftware/fitsstorage/fitsstorageutils:$LABEL
  fi
fi

popd
