#!/bin/bash

# Build fresh images
docker image build -t gemini/fitsarchiveutils:jenkins -f docker/fitsstorage-jenkins/Dockerfile .
docker image build -t gemini/archive:jenkins -f docker/archive-jenkins/Dockerfile .

# Cleanup any residuals
docker network create fitsstorage-jenkins || true
docker container rm fitsdata-jekins || true
docker container rm archive-jenkins || true
docker container rm archive-tester || true

# Start Postgres
# Create the databases
docker run --network fitsstorage-jenkins --name fitsdata-jenkins -e POSTGRES_USER=fitsdata -e POSTGRES_PASSWORD=fitsdata -e POSTGRES_DB=fitsdata -d postgres:12

# Start Archive
docker run --network fitsstorage-jenkins --name archive-jenkins -e FITS_DB_SERVER="fitsdata:fitsdata@fitsdata-jenkins" -e TEST_IMAGE_PATH=/tmp/archive_test_images -e TEST_IMAGE_CACHE=/tmp/cached_archive_test_images -e CREATE_TEST_DB=False -e PYTHONPATH=/opt/FitsStorage:/opt/DRAGONS -d gemini/archive:jenkins

# Run Table init and tests
docker run --name archive-tester  --network fitsstorage-jenkins -e FITS_DB_SERVER="fitsdata:fitsdata@fitsdata-jenkins" -e PYTEST_SERVER=http://archive-jenkins -e TEST_IMAGE_PATH=/tmp/archive_test_images -e TEST_IMAGE_CACHE=/tmp/cached_archive_test_images -e CREATE_TEST_DB=False -e PYTHONPATH=/opt/FitsStorage:/opt/DRAGONS -d gemini/fitsarchiveutils:jenkins bash -c "python fits_storage/scripts/create_tables.py; mkdir -p /tmp/archive_test_images; mkdir -p /tmp/cached_archive_test_images; cd /opt/FitsStorage && pytest tests "

docker container stop archive-jenkins
docker container stop fitsdata-jenkins
docker container rm archive-jenkins
docker container rm fitsdata-jenkins
