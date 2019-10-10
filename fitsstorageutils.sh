#!/bin/bash
docker run --name fitsstorageutils --network fitsstorage -e FITS_DB_SERVER="fitsdata:fitsdata@postgres-fitsdata" -it fitsstorageutils:latest
