#!/bin/bash
BASEDIR=$(dirname "$0")
UPLOAD=$1
pushd "$BASEDIR/../../.."

BRANCH=`git rev-parse --abbrev-ref HEAD`
echo Branch is $BRANCH
if [[ "$BRANCH" == "2020-2" ]]
then
  LABEL="2020-2"
else
  LABEL="latest"
fi

docker image build -t fitsimage:$LABEL -f FitsStorage/docker/archive//Dockerfile .

if [[ $UPLOAD == "-u" ]]
then
  if [[ "$BRANCH" == "master" ]]
  then
    echo "setting label to $LABEL"
    docker login gitlab.gemini.edu:4567
    docker tag fitsimage:$LABEL gitlab.gemini.edu:4567/drsoftware/fitsstorage/fitsimage:$LABEL
    docker push gitlab.gemini.edu:4567/drsoftware/fitsstorage/fitsimage:$LABEL
  elif [[ "$BRANCH" == "2020-1" ]]
  then
    echo "setting label to 2020-1"
  fi
fi

popd
