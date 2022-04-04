#! /bin/bash

cd /tmp/jenkins_pytest/dataflow/

for file in \
  02nov12.0440.fits \
  2002APR26_006.fits \
  2003jun29_3341.fits \
  2006aug04_1.3042.fits \
  20130711_GN-2013B-Q-60_obslog.txt \
  2019mar22_science018.fits \
  GN2013BQ052-03.fits \
  GN2013BQ052-04.fits \
  N20111109S0165.fits \
  N20111109S0172.fits \
  N20111109S0176.fits \
  N20111117S0104.fits \
  N20120505S0018.fits \
  N20120505S0025.fits \
  N20120505S0035.fits \
  N20120505S0075.fits \
  N20120505S0144.fits \
  N20120505S0195.fits \
  N20120505S0331.fits \
  N20120629S0171.fits \
  N20120629S0179.fits \
  N20130711S0203.fits \
  N20130712S0005.fits \
  N20130712S0041.fits \
  N20130712S0116.fits \
  N20140505S0102.fits \
  N20140505S0110.fits \
  N20140505S0111.fits \
  N20140505S0118.fits \
  N20140505S0165.fits \
  N20170614S0179.fits \
  N20190620S0573.fits \
  N20190628G0024.fits \
  N20190701S0019.fits \
  N20190701S0724.fits \
  N20190719S0005.fits \
  N20190719S0006.fits \
  N20190719S0007.fits \
  N20190719S0008.fits \
  N20190719S0011.fits \
  N20190719S0012.fits \
  N20190719S0045.fits \
  S20070131S0145.fits \
  S20111124S0069.fits \
  S20120401S0218.fits \
  S20120605S0127.fits \
  S20130201S0158.fits \
  S20130201S0173.fits \
  S20130202S0349.fits \
  S20130712S0078.fits \
  S20130712S0088.fits \
  S20130712S0102.fits \
  S20130712S0113.fits \
  S20130712S0118.fits \
  S20130806S0071.fits \
  S20150501S0127.fits \
  S20150501S0128.fits \
  S20150501S0446.fits \
  S20150505S0292.fits \
  S20180305S0039.fits \
  S20190811S0226.fits \
  S20190811S0227.fits \
  S20190811S0862.fits \
  S20190812S0005.fits \
  TX20170321_raw.2504.fits \
  r01dec10_008.fits \
  rS20120605S0120.fits
  do
    test -e /tmp/jenkins_pytest/dataflow/$file || wget --no-check-certificate https://archive.gemini.edu/file/$file
  done

echo "Done loading files"
ls /tmp/jenkins_pytest/dataflow/
