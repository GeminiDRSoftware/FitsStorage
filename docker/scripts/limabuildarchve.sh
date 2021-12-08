#!/bin/bash
BASEDIR=$(dirname "$0")
UPLOAD=$1
pushd "$BASEDIR/../../.."

BRANCH=`git rev-parse --abbrev-ref HEAD`
echo Branch is $BRANCH
if [[ "$BRANCH" == "2020-2" ]]
then
  LABEL="2020-2"
elif [[ "$BRANCH" == "2021-1" ]]
then
  LABEL="2021-1"
elif [[ "$BRANCH" == "2021-2" ]]
then
  LABEL="2021-2"
elif [[ "$BRANCH" == "2022-1" ]]
then
  LABEL="2022-1"
else
  LABEL="latest"
fi

#lima nerdctl build -t fitsimage:$LABEL -f FitsStorage/docker/archive//Dockerfile .
#lima nerdctl build -f FitsStorage/docker/archive//Dockerfile . -t fitsimage:latest
lima nerdctl build -f FitsStorage/docker/archive-lima/Dockerfile . -t fitsimage:$LABEL

popd
