#!/bin/bash
docker run --name fitsstorageutils --network fitsstorage -v ~/data:/data/upload_staging -v ~/dataflow:/sci/dataflow -e FITS_DB_SERVER="fitsdata:fitsdata@postgres-fitsdata" --rm -it fitsstorageutils:latest $*
