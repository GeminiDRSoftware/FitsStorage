#!/bin/bash
DATABASE="fitsdata:fitsdata@postgres-archive"

docker run --name api --network fitsstorage -v ~/testdata/archive-data-upload_staging:/data/upload_staging -v ~/testdata/archive-sci-dataflow:/sci/dataflow -e FITS_DB_SERVER="$DATABASE" -e USE_AS_ARCHIVE=True -e API_BACKEND_LOCATION=':8000' -d fitsimage:latest python3 fits_storage/scripts/api_backend.py

