#! /usr/bin/env python

import urllib2
from xml.dom.minidom import parseString
from optparse import OptionParser
from datetime import datetime
from dateutil.parser import parse as parsedate

from fits_storage.orm import session_scope
from fits_storage.logger import logger, setdebug, setdemon

from fits_storage.utils.notifications import ingest_odb_xml
from fits_storage.fits_storage_config import magic_download_cookie
from fits_storage.apache_return_codes import HTTP_OK

parser = OptionParser()
parser.add_option("--odb", action="store", dest="odb", help="ODB server to query. Probably gnodb or gsodb")
parser.add_option("--semester", action="store", dest="semester", default=None, help="Query ODB for only the given semester. Use auto to automatically get current and previous semesters")
parser.add_option("--to-remote-server", action="store", dest="to_remote_server", help="Upload notifications via http to this remote server and do not ingest locally if specified. If not specified, ingest locally")
parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run as a background demon, do not generate stdout")
parser.add_option("--fakedate", action="store", dest="faked", default=None, help="Fake the current date, for test purposes")
(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

def download_and_ingest(url):
    logger.info("Fetching XML from ODB server: %s", url)
    xml = urllib2.urlopen(url).read()
    logger.debug("Got %d bytes from server.", len(xml))

    # Upload to remote, or ingest locally?
    if options.to_remote_server:
        url = "%s/import_odb_notifications" % options.to_remote_server
        opener = urllib2.build_opener()
        opener.addheaders.append(('Cookie', 'gemini_fits_authorization=%s' % magic_download_cookie))
        u = opener.open(url, xml)
        report = u.read()
        u.close()
        if u.getcode() != HTTP_OK:
            logger.error("Got not-OK return code from remote server: %s", u.getcode())

        # Make the report into a list of lines for the log
        try:
            report = report.split('\n')
        except:
            report = []

    else:
        with session_scope() as session:
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

url = "http://%s:8442/odbbrowser/programs" % options.odb
if options.semester == 'auto':
    # When in "auto" mode, we want to ingest the "current" and "past" semesters,
    # using some heuristics based on the current date (or a fake current date,
    # typically for test purposes).
    if options.faked is not None:
        now = parsedate(options.faked)
    else:
        now = datetime.now()
    jun1st = datetime(now.year, 6, 1)
    dec1st = datetime(now.year, 12, 1)
    if now >= jun1st and now < dec1st:
        # We are by the end of period A, or within period B
        # Ask for current year's period A + period B
        period_years = (now.year, now.year)
    elif now >= dec1st:
        # We're by the end of this year's period B
        period_years = (now.year + 1, now.year)
    else:
        # We're by the end of PAST year's period B
        period_years = (now.year, now.year - 1)
    logger.info("Auto semester - will do %sA, %sB", period_years[0], period_years[1])
    download_and_ingest(url + '?programSemester={}A'.format(period_years[0]))
    download_and_ingest(url + '?programSemester={}B'.format(period_years[1]))
else:
    if options.semester is not None:
        url += "?programSemester=%s" % options.semester
    download_and_ingest(url)
