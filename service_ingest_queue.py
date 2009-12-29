import sys
sys.path=['/opt/sqlalchemy/lib/python2.5/site-packages', '/astro/iraf/x86_64/gempylocal/lib/stsci_python/lib/python2.5/site-packages']+sys.path

import FitsStorage
from FitsStorageUtils import *
import os
import re
import datetime
import time

from optparse import OptionParser

parser = OptionParser()
parser.add_option("--force-crc", action="store_true", dest="force_crc", help="Force crc check on pre-existing files")
parser.add_option("--skip-fv", action="store_true", dest="skip_fv", help="Do not run fitsverify on the files")
parser.add_option("--skip-wmd", action="store_true", dest="skip_wmd", help="Do not run a wmd check on the files")

(options, args) = parser.parse_args()

# Annouce startup
now = datetime.datetime.now()
startup = "*********  service_ingest_queue.py - starting up at %s" % now
print "\n\n%s\n" % startup

session = sessionfactory()

# Go into loop. should there be an exit clause?
while(1):
  # Request a queue entry
  iq = pop_ingestqueue(session)

  if(not iq):
    print "Nothing on queue. Waiting"
    time.sleep(30)
  else:
    print "Ingesting %s" % iq.filename
    session.flush()
    try:
      ingest_file(session, iq.filename, iq.path, options.force_crc, options.skip_fv, options.skip_wmd)
      session.delete(iq)
      session.commit()
    except:
      session.rollback()

session.close()
