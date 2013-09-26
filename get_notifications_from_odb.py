#! /usr/bin/env python
from FitsStorage import *
import FitsStorageConfig
from FitsStorageLogger import *

import urllib2
from xml.dom.minidom import parseString

from optparse import OptionParser

parser = OptionParser()
parser.add_option("--odb", action="store", dest="odb", help="ODB server to query. Probably gnodb or gsodb")
parser.add_option("--semester", action="store", dest="semester", help="Query ODB for only the given semester")
parser.add_option("--dryrun", action="store_true", dest="dryrun", default=False, help="Do not actually update notification, just say what would be done")
parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run as a background demon, do not generate stdout")
(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

url = "http://%s:8296/odbbrowser/programs" % options.odb
if(options.semester):
  url+="?programSemester=%s" % options.semester
logger.info("Fetching XML from ODB server: %s", url)
u = urllib2.urlopen(url)
xml = u.read()
u.close()
logger.debug("Got %d bytes from server. Parsing." % len(xml))

# Get a database session
session=sessionfactory()

nprogs=0
dom = parseString(xml)
for pe in dom.getElementsByTagName("program"):
  readok=True
  try:
    progid=pe.getElementsByTagName("reference")[0].childNodes[0].data
    logger.debug("got %s" % progid)
    piEmail=pe.getElementsByTagName("piEmail")[0].childNodes[0].data
    ngoEmail=pe.getElementsByTagName("ngoEmail")[0].childNodes[0].data
    csEmail=pe.getElementsByTagName("contactScientistEmail")[0].childNodes[0].data
    notifyPi=pe.getElementsByTagName("notifyPi")[0].childNodes[0].data
    nprogs+=1
  except:
    logger.debug("Failed to process program node")
    readok = False

  if(readok):
    # Search for this program ID in notification table
    label = "Auto - %s" % progid
    query = session.query(Notification).filter(Notification.label==label)
    if(query.count()==0):
      n = Notification(label)
      n.selection = "%s/science" % progid
      n.to = piEmail
      n.cc = "%s,%s" % (ngoEmail, csEmail)
      if(not options.dryrun):
        logger.info("Adding notification %s" % label)
        session.add(n)
        session.commit()
      else:
        logger.info("Dryrun mode - not really adding %s" % label)
    else:
      logger.info("%s is already present, check for updates" % label)
      n = query.first()
      if(n.to != piEmail):
        if(not options.dryrun):
          logger.info("Updating to for %s" % label)
          n.to = piEmail
          session.commit()
        else:
          logger.info("Dryrun - not actually updating Email to for %s" % label)
      if(n.cc != "%s,%s" % (ngoEmail, csEmail)):
        if(not options.dryrun):
          logger.info("Updating cc for %s" % label)
          n.cc = "%s,%s" % (ngoEmail, csEmail)
          session.commit()
        else:
          logger.info("Dryrun - not actually updating Email CC for %s" % label)
    
logger.info("Processed %s programs" % nprogs)
