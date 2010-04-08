import sys
sys.path=['/opt/sqlalchemy/lib/python2.5/site-packages', '/astro/iraf/x86_64/gempylocal/lib/stsci_python/lib/python2.5/site-packages']+sys.path

import FitsStorage
import FitsStorageConfig
import FitsStorageCal
from FitsStorageUtils import *
from FitsStorageLogger import *
import GeminiMetadataUtils
import os
import sys
import re
import datetime
import dateutil
import time

from optparse import OptionParser

parser = OptionParser()
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--date", action="store", dest="date", default="today", help="date in question")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)


# Parse the date option we got.
yyyymmdd=GeminiMetadataUtils.gemini_date(options.date)
startdt = dateutil.parser.parse("%s 00:00:00" % yyyymmdd)
oneday = datetime.timedelta(days=1)
enddt = startdt + oneday

print "Date: %s" % yyyymmdd

session = sessionfactory()

try: 
  # First get a list of all the present GMOS spectroscopy OBJECT files on the given UTDATE
  oquery = session.query(Header).select_from(join(Header, DiskFile)).filter(DiskFile.present==True).filter(Header.instrument=='GMOS-N').filter(Header.spectroscopy==True).filter(Header.obstype=='OBJECT').filter(Header.utdatetime >= startdt).filter(Header.utdatetime <= enddt)

  nobjects = oquery.count()
  print "Found %d OBJECTs requiring ARCs" % nobjects

  objects = oquery.all()

  for object in objects:
    # Find an arc for this header
    c = FitsStorageCal.Calibration(session, None, object)
    print "OBJECT file: %s (%s)" % (object.diskfile.file.filename, object.datalab)

    arc = c.arc()

    if(arc):
      interval = arc.utdatetime - object.utdatetime
      hours = (interval.days * 24.0) + (interval.seconds / 3600.0)
      word = "after"
      if(hours < 0.0):
        word = "before"
        hours *= -1.0
      print "-- ARC: %s (%s: %.1f hours %s)" % (arc.diskfile.file.filename, arc.datalab, hours, word)
    else:
      print "No Arc Found"


finally:
  session.close()
