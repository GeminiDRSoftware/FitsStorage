#! /bin/bash

# Put the data in FitsStorage/testdata/ so the docker will copy it in
# This is at: /tmp/jenkins_pytest/dataflow

cd FitsStorage/testdata/

wget --no-check-certificate https://archive.gemini.edu/file/N20130711S0203.fits
