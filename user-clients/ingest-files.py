#! /usr/bin/env python

#************************************************************************
#****              G E M I N I  O B S E R V A T O R Y                ****
#************************************************************************
#
#   Script name:        ingest-files
#
#   Purpose:
#      Add files to the Archive ingest queue. This is done periodically
#      for files produced by the observing system, in an automatic way,
#      but sometimes one cannot wait for the system to trigger the
#      reingestion.
#
#      Another use for this is to ingest files that sit on subdirecto-
#      ries that are not monitored.
#
#   Date               : 2015-10-16
#
#   Author             : Ricardo Cardenes
#
#   Modification History:
#    2015-10-16, rcardene : First release
#

import urllib.request, urllib.parse, urllib.error
import json
from contextlib import closing
import logging

SERVER = 'fits'

logger = logging.getLogger('Ingesting')
logging.basicConfig(format='%(name)s... %(levelname)s: %(message)s')

def post_query(url, query_data):
    """
    Performs a query against a web service expecting some JSON object to
    be returned.

    Transform the JSON string into a Python object, and return it to the
    caller
    """
    try:
        with closing(urllib.request.urlopen(url, data=json.dumps(query_data))) as response:
            status = response.getcode()
            if status == 200:
                return json.loads(response.read())
            if 400 <= status < 500:
                logger.error("Could not access the web server. Maybe misconfigured script. Please, report the problem")
            else:
                logger.error("Got some non-specific error when querying the server. Report this!")
    # We get ValueError if the query returns a non-valid JSON object.
    # We get the other two errors if something went wrong with querying the web server
    except ValueError:
        logger.error("Could not retrieve valid information from the server")
    except IOError:
        logger.error("Could not contact the web server!")

#########################################################################################################################################

# Option Parsing
from argparse import ArgumentParser
parser = ArgumentParser(description="Ask the Archive to ingest files")
parser.add_argument("--path", action="store", dest="path", help="Use given path relative to storage root")
parser.add_argument("--force", action="store_true", dest="force", help="Force re-ingestion of these files unconditionally")
parser.add_argument("--force_md5", action="store_true", dest="force_md5", help="Force checking of file change by md5 not just lastmod date")
parser.add_argument("filepref", metavar='prefix', help="Prefix for the files that need to be added")

options = parser.parse_args()

query = dict(
    filepre   = options.filepref,
    path      = options.path or '',
    force     = options.force,
    force_md5 = options.force_md5
)

url = "http://{server}/ingest_files".format(server = SERVER)
ret = post_query(url, query)
if ret:
    if 'error' in ret:
        logger.error(ret['error'])
    else:
        for filename in ret['added']:
            print(("Added {}".format(filename)))
