#!/bin/bash
BASEDIR=$(dirname "$0")
UPLOAD=$1
pushd "$BASEDIR/../../.."

BRANCH=`cd FitsStorage && git rev-parse --abbrev-ref HEAD`
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

echo `pwd`
echo $LABEL
# lima nerdctl build -t fitsstorageutils:$LABEL -f FitsStorage/docker/fitsstorage/Dockerfile .
lima nerdctl build -f FitsStorage/docker/fitsstorage/Dockerfile . -t fitsstorageutils:$LABEL

popd
