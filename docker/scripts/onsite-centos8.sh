#!/bin/bash
ARCHIVE=onsite-centos8
HTTP_PORT=8080
HTTPS_PORT=8443
DATABASE="fitsdata:fitsdata@postgres-onsite"
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

# Cleanup if required
docker container ps -a | grep onsite-centos8 && (docker container stop onsite-centos8 ; docker container rm onsite-centos8)
docker container ps -a | grep onsite_ingest && (docker container stop onsite_ingest ; docker container rm onsite_ingest)
docker container ps -a | grep onsite_export && (docker container stop onsite_export ; docker container rm onsite_export)
docker container ps -a | grep onsite_add_to_ingest_queue && (docker container stop onsite_add_to_ingest_queue ; docker container rm onsite_add_to_ingest_queue)

# Start the webserver
docker run --name "$ARCHIVE" --network fitsstorage -v ~/testdata/onsite-data-upload_staging:/data/upload_staging -v ~/testdata/onsite-sci-dataflow:/sci/dataflow -e FITS_DB_SERVER="$DATABASE" -e USE_AS_ARCHIVE="False" -e EXPORT_DESTINATIONS="http://archive" --publish $HTTP_PORT:80 --publish $HTTPS_PORT:443 -d fitsimage:centos8

# Run the add to ingest queue
docker run --name onsite_add_to_ingest_queue --network fitsstorage -v ~/testdata/onsite-data-upload_staging:/data/upload_staging -v ~/testdata/onsite-sci-dataflow:/sci/dataflow -e FITS_DB_SERVER="fitsdata:fitsdata@postgres-onsite" -e EXPORT_DESTINATIONS="http://archive" --rm -it fitsstorageutils:centos8 python3 fits_storage/scripts/add_to_ingest_queue.py

# Run the ingest service (continuously)
docker run --name onsite_ingest --network fitsstorage -v ~/testdata/onsite-data-upload_staging:/data/upload_staging -v ~/testdata/onsite-sci-dataflow:/sci/dataflow -e FITS_DB_SERVER="fitsdata:fitsdata@postgres-onsite" -e EXPORT_DESTINATIONS="http://archive" -d fitsstorageutils:centos8 python3 fits_storage/scripts/service_ingest_queue.py

# Run the export service (continuously)
docker run --name onsite_export --network fitsstorage -v ~/testdata/onsite-data-upload_staging:/data/upload_staging -v ~/testdata/onsite-sci-dataflow:/sci/dataflow -e FITS_DB_SERVER="fitsdata:fitsdata@postgres-onsite" -e EXPORT_DESTINATIONS="http://archive" -d fitsstorageutils:centos8 python3 fits_storage/scripts/service_export_queue.py

# This seems to be automatic based on server config for export destinations
# # Run the add to export queue
# docker run --name onsite_add_to_export_queue --network fitsstorage -v ~/testdata/onsite-data-upload_staging:/data/upload_staging -v ~/testdata/onsite-sci-dataflow:/sci/dataflow -e FITS_DB_SERVER="fitsdata:fitsdata@postgres-onsite" -e EXPORT_DESTINATIONS="http://archive" --rm -it fitsstorageutils:centos8 python3 fits_storage/scripts/add_to_export_queue.py --destination "http://archive-centos8"
