#!/bin/bash
docker run --name fitsstorageutils-centos8 --network onsite -v ~/data:/data/upload_staging -v ~/dataflow:/sci/dataflow -e FITS_DB_SERVER="fitsdata:fitsdata@postgres-onsite" --rm -it fitsstorageutils:centos8
