#! /bin/bash

cd /tmp/jenkins_pytest/dataflow/

for file in \
  N20130711S0203.fits \
  20130711_GN-2013B-Q-60_obslog.txt \
  N20120629S0171.fits \
  S20070131S0145.fits \
  2003jun29_3341.fits
  do
    rm -f /tmp/jenkins_pytest/dataflow/$file
    wget --no-check-certificate https://archive.gemini.edu/file/$file
  done

echo "Done loading files"
ls /tmp/jenkins_pytest/dataflow/
