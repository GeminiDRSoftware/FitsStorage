#! /usr/bin/env python3
import http
from optparse import OptionParser
from datetime import datetime

import requests
import requests.utils
import json

from fits_storage.logger import logger, setdebug, setdemon

from fits_storage.gemini_metadata_utils import gemini_semester, \
    previous_semester

from fits_storage.server.odb_program_interface import get_odb_prog_dicts
from fits_storage.server.odb_data_handlers import update_notifications, \
    update_programs

from fits_storage.db import session_scope

from fits_storage.config import get_config


fsc = get_config()


parser = OptionParser()
parser.add_option("--odb", action="store", dest="odb",
                  help="ODB server to query. Probably gnodb or gsodb. This is"
                       "used directly as the hostname in the URL")
parser.add_option("--active", action="store_true", dest="active",
                  default=False, help="Query ODB for all active programs. "
                                      "Overrides --semester")
parser.add_option("--odb_notifypi", action="store_true", dest="notifypi",
                  default=False, help="sets programNotifyPi=true in the ODB "
                                      "query URL")
parser.add_option("--semester", action="store", dest="semester", default=None,
                  help="Query ODB for only the given semester, eg 2021A. You "
                       "can also use 'current' or 'previous'")
parser.add_option("--relay-to", action="store", dest="relayto",
                  help="Relay notifications via http to this remote server "
                       "and do not ingest locally if specified. "
                       "If not specified, ingest locally")
parser.add_option("--dryrun", action="store_true", dest="dryrun", default=False,
                  help="Download the ODB data but do not actually ingest or"
                       "forward it")
parser.add_option("--xmlfile", action="store", dest="xmlfile",
                  help="XML file to use instead of querying an ODB. Used "
                       "mostly for testing.")
parser.add_option("--debug", action="store_true", dest="debug", default=False,
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False,
                  help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Parse special semester values
if options.semester in ('current', 'previous'):
    semester = gemini_semester(datetime.utcnow())
    if options.semester == 'previous':
        semester = previous_semester(semester)
elif options.semester:
    semester = options.semester
else:
    semester = None

# Check for sane options
if options.odb is None and options.xmlfile is None:
    logger.error("No ODB specified, exiting")
    exit()

# Are we using an xml inject?
if options.xmlfile:
    with open(options.xmlfile) as f:
        xml = f.read()
else:
    xml = None

# Get the programs as a list of dictionaries
programs = get_odb_prog_dicts(options.odb, semester, options.active,
                              options.notifypi, logger, xml_inject=xml)
if programs is None:
    logger.error("Failed to get programs from ODB. Exiting")
    exit(1)
logger.info("Got %d programs from ODB", len(programs))

if options.dryrun:
    logger.info("Dry Run only, exiting now")
    exit()

# Are we ingesting locally, or relaying to a remote host?
if options.relayto:
    logger.info("Relaying programs to remote host: %s", options.relayto)
    rs = requests.Session()
    cookie_dict = {'gemini_fits_upload_auth': fsc.export_auth_cookie}
    requests.utils.add_dict_to_cookiejar(rs.cookies, cookie_dict)
    url = "%s/ingest_programs" % options.relayto
    data = json.dumps(programs)
    try:
        req = rs.post(url, data=data, timeout=120)
    except requests.Timeout:
        logger.error(f"Timeout posting {url}", exc_info=True)
        exit()
    except requests.ConnectionError:
        logger.error(f"ConnectionError posting {url}", exc_info=True)
        exit()
    except requests.RequestException:
        logger.error(f"RequestException posting {url}", exc_info=True)
        exit()

    logger.debug("Got http status %s and response %s",
                 req.status_code, req.text)

    if req.status_code != http.HTTPStatus.OK:
        logger.error("Got bad HTTP status %s POSTing to %s", req.status_code,
                     url)
    else:
        logger.info("POSTed ODB successfully to %s", url)
else:
    with session_scope() as session:
        update_programs(session, programs, logger)
        update_notifications(session, programs, logger)
