#! /usr/bin/env python

import urllib2
from xml.dom.minidom import parseString
from optparse import OptionParser

from orm import sessionfactory
from logger import logger, setdebug, setdemon

from utils.notifications import ingest_odb_xml

parser = OptionParser()
parser.add_option("--odb", action="store", dest="odb", help="ODB server to query. Probably gnodb or gsodb")
parser.add_option("--semester", action="store", dest="semester", help="Query ODB for only the given semester")
parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run as a background demon, do not generate stdout")
(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

url = "http://%s:8442/odbbrowser/programs" % options.odb
if options.semester:
    url += "?programSemester=%s" % options.semester
logger.info("Fetching XML from ODB server: %s", url)
u = urllib2.urlopen(url)
xml = u.read()
u.close()
logger.debug("Got %d bytes from server. Parsing.", len(xml))

# Get a database session
session = sessionfactory()

report = ingest_odb_xml(session, xml)

# Replay the report into the log files
for l in report:
    if l.startswith("ERROR"):
        logger.error(l)
    else:
        logger.info(l)
