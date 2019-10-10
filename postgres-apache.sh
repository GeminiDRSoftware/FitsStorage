#!/bin/bash
docker run --name postgres-apache --network fitsstorage -e POSTGRES_USER=apache -e POSTGRES_PASSWORD=apache -e POSTGRES_DB=apache -d postgres:12
