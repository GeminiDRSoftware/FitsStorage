#!/bin/bash
ARCHIVE=archive-centos8
HTTP_PORT=80
HTTPS_PORT=443
DATABASE="fitsdata:fitsdata@postgres-fitsdata"
if [ "" != "$1" ]; then
    ARCHIVE=$1
fi
if [ "" != "$2" ]; then
    HTTP_PORT=$2
fi
if [ "" != "$3" ]; then
    HTTPS_PORT=$3
fi
if [ "" != "$4" ]; then
    DATABASE=$4
fi
docker run --name "$ARCHIVE" --network fitsstorage -v ~/data:/data/upload_staging -v ~/dataflow:/sci/dataflow -e FITS_DB_SERVER="$DATABASE" --publish $HTTP_PORT:80 --publish $HTTPS_PORT:443 -d fitsimage:centos8
