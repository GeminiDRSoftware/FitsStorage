#! /usr/bin/env python

import urllib2
from xml.dom.minidom import parseString
from optparse import OptionParser

from orm import sessionfactory
from logger import logger, setdebug, setdemon

from utils.notifications import ingest_odb_xml
from fits_storage_config import magic_download_cookie
import apache_return_codes as apache

parser = OptionParser()
parser.add_option("--odb", action="store", dest="odb", help="ODB server to query. Probably gnodb or gsodb")
parser.add_option("--semester", action="store", dest="semester", help="Query ODB for only the given semester")
parser.add_option("--to-remote-server", action="store", dest="to_remote_server", help="Upload notifications via http to this remote server and do not ingest locally if specified. If not specified, ingest locally")
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
logger.debug("Got %d bytes from server.", len(xml))

# Upload to remote, or ingest locally?
if options.to_remote_server:
    url = "http://%s/import_odb_notifications" % options.to_remote_server
    opener = urllib2.build_opener()
    opener.addheaders.append(('Cookie', 'gemini_fits_authorization=%s' % magic_download_cookie))
    u = opener.open(url, xml)
    report = u.read()
    u.close()
    if u.getcode() != apache.OK:
        logger.error("Got not-OK return code from remote server: %s", u.getcode())

    # Make the report into a list of lines for the log
    try:
        report = report.split('\n')
    except:
        report = []

else:
    # Get a database session
    session = sessionfactory()

    # Do the actual ingest
    report = ingest_odb_xml(session, xml)

# Replay the report into the log files
server = "local"
if options.to_remote_server:
    server = options.to_remote_server

for l in report:
    if l.startswith("ERROR"):
        logger.error("%s: %s", server, l)
    else:
        logger.info("%s: %s", server, l)
