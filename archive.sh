#!/bin/bash
docker run --name archive --network fitsstorage -v ~/data:/data/upload_staging -v ~/dataflow:/sci/dataflow -e FITS_DB_SERVER="fitsdata:fitsdata@postgres-fitsdata" --publish 80:80 --publish 443:443 -d fitsimage:latest
