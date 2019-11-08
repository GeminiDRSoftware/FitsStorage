#!/bin/bash
docker container stop archive_ingest archive onsite_export onsite_ingest onsite_archive postgres-archive postgres-onsite api
docker container rm archive_ingest archive onsite_export onsite_ingest onsite_archive postgres-archive postgres-onsite api
rm -rf ~/testdata/archive-data-upload_staging/*
