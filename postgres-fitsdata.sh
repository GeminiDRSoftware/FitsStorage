#!/bin/bash
docker run --name postgres-fitsdata --network fitsstorage -e POSTGRES_USER=fitsdata -e POSTGRES_PASSWORD=fitsdata -e POSTGRES_DB=fitsdata -d postgres:12
