#! /usr/bin/env python3

import requests
import http
from optparse import OptionParser
from datetime import datetime
from dateutil.parser import parse as parsedate

from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.gemini_metadata_utils import gemini_semester, \
    previous_semester
from fits_storage.utils.notifications import ingest_odb_xml
from fits_storage.fits_storage_config import magic_download_cookie

from gemini_obs_db.db import session_scope


def download_and_ingest(url):
    logger.info("Fetching XML from ODB server: %s", url)
    r = requests.get(url)
    xml = r.text
    logger.debug("Got %d bytes from server.", len(xml))
    if r.status_code != http.HTTPStatus.OK:
        logger.error("Got bad http status code from ODB: %s", r.status_code)
        return

    # Upload to remote, or ingest locally?
    if options.to_remote_server:
        cookies = dict(gemini_fits_authorization=magic_download_cookie)
        r = requests.get(url, cookies=cookies)
        url = "%s/import_odb_notifications" % options.to_remote_server
        r = requests.post(url, data=xml)
        report = r.text
        if r.status_code != http.HTTPStatus.OK
            logger.error("Got not-OK return code from remote server: %s", r.status_code)

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



parser = OptionParser()
parser.add_option("--odb", action="store", dest="odb",
                  help="ODB server to query. Probably gnodb or gsodb. This is"
                       "used directly as the hostname in a URL")
parser.add_option("--active", action="store_true", dest="all_active",
                  default=False, help="Query ODB for all active programs. "
                                      "Overrides --semester")
parser.add_option("--semester", action="store", dest="semester", default=None,
                  help="Query ODB for only the given semester. Use 'auto' to "
                       "automatically get current and previous semesters")
parser.add_option("--to-remote-server", action="store", dest="to_remote_server",
                  help="Upload notifications via http to this remote server "
                       "and do not ingest locally if specified. "
                       "If not specified, ingest locally")
parser.add_option("--fakedate", action="store", dest="faked", default=None,
                  help="Fake the current date, for test purposes")
parser.add_option("--dryrun", action="store_true", dest="dryrun", default=False,
                  help="Download the ODB data but do not actually ingest or"
                       "forward it")
parser.add_option("--debug", action="store_true", dest="debug", default=False,
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False,
                  help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

url = "http://%s:8442/odbbrowser/programs" % options.odb
if options.all_active:
    logger.info("Retrieving all active programs")
    download_and_ingest(
        url + '?programSemester=20*&programNotifyPi=true&programActive=yes')
elif options.semester == 'auto':
    # When in "auto" mode, we want to ingest the "current" and "past" semesters,
    # using some heuristics based on the current date (or a fake current date,
    # typically for test purposes).
    if options.faked is not None:
        now = parsedate(options.faked)
    else:
        now = datetime.now()
    semester = gemini_semester(now)
    previous = previous_semester(semester)
    logger.info("Auto semester - will do %s, %s", semester, previous)
    download_and_ingest(url + '?programSemester=%s' % semester)
    download_and_ingest(url + '?programSemester=%s' % previous)
else:
    if options.semester is not None:
        logger.info("Retrieving only semester %s", options.semester)
        download_and_ingest(url + "?programSemester=%s" % options.semester)
    else:
        logger.info("No semester selected")
