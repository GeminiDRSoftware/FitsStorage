#!/bin/bash
BASEDIR=$(dirname "$0")
UPLOAD=$1
pushd "$BASEDIR/../../.."

BRANCH=`cd FitsStorageServices && git rev-parse --abbrev-ref HEAD`
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
elif [[ "$BRANCH" == "2022-2" ]]
then
  LABEL="2022-2"
else
  LABEL="latest"
fi

docker image build -t fitsservices:$LABEL -f FitsStorage/docker/fitsstorageservices/Dockerfile .

if [[ $UPLOAD == "-u" ]]
then
  if [[ "$BRANCH" == "master" ]]
  then
    echo "setting label to $LABEL"
    docker login gitlab.gemini.edu:4567
    docker tag fitsservices:$LABEL gitlab.gemini.edu:4567/drsoftware/fitsstorage/fitsservices:$LABEL
    docker push gitlab.gemini.edu:4567/drsoftware/fitsstorage/fitsservices:$LABEL
  else
    echo "setting label to $BRANCH"
  fi
fi

popd
