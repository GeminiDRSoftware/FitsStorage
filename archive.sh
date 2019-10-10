#!/bin/bash
docker run --name archive --network fitsstorage -e FITS_DB_SERVER="fitsdata:fitsdata@postgres-fitsdata" --publish 80:80 --publish 443:443 -d fitsimage:latest
