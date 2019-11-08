#!/bin/bash
ARCHIVE=archive
HTTP_PORT=80
HTTPS_PORT=443
DATABASE="fitsdata:fitsdata@postgres-archive"
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
docker run --name "$ARCHIVE" --network fitsstorage -v ~/testdata/archive-data-upload_staging:/data/upload_staging -v ~/testdata/archive-sci-dataflow:/sci/dataflow -e FITS_DB_SERVER="$DATABASE" -e USE_AS_ARCHIVE=True -e API_BACKEND_LOCATION="api:8000" --publish $HTTP_PORT:80 --publish $HTTPS_PORT:443 -d fitsimage:centos8

# # Run the add to ingest queue
# docker run --name archive_add_to_ingest_queue --network fitsstorage -v ~/testdata/archive-data-upload_staging:/data/upload_staging -v ~/testdata/archive-sci-dataflow:/sci/dataflow -e FITS_DB_SERVER="fitsdata:fitsdata@postgres-archive" -e API_BACKEND_LOCATION="api:8000" --rm -it fitsstorageutils:centos8 python3 fits_storage/scripts/add_to_ingest_queue.py

# Run the ingest service (continuously)
docker run --name archive_ingest --network fitsstorage -v ~/testdata/archive-data-upload_staging:/data/upload_staging -v ~/testdata/archive-sci-dataflow:/sci/dataflow -e FITS_DB_SERVER="fitsdata:fitsdata@postgres-archive" -e API_BACKEND_LOCATION="api:8000" -d fitsstorageutils:centos8 python3 fits_storage/scripts/service_ingest_queue.py
