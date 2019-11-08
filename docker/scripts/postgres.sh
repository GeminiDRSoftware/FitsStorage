#!/bin/bash

# Create the networks
docker network ls | grep fitsstorage || docker network create fitsstorage

# Create the databases
docker run --name postgres-onsite --network fitsstorage -e POSTGRES_USER=fitsdata -e POSTGRES_PASSWORD=fitsdata -e POSTGRES_DB=fitsdata -d postgres:12
docker run --name postgres-archive --network fitsstorage -e POSTGRES_USER=fitsdata -e POSTGRES_PASSWORD=fitsdata -e POSTGRES_DB=fitsdata -d postgres:12

#!/bin/bash
docker run --name createtables --network fitsstorage -e FITS_DB_SERVER="fitsdata:fitsdata@postgres-onsite" --rm -it fitsstorageutils:latest python3 ./fits_storage/scripts/create_tables.py
docker run --name createtables --network fitsstorage -e FITS_DB_SERVER="fitsdata:fitsdata@postgres-archive" --rm -it fitsstorageutils:latest python3 ./fits_storage/scripts/create_tables.py
