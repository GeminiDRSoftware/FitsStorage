#!/usr/bin/env python
#                                                                            GOA
#                                                                    odb_data.py
# ------------------------------------------------------------------------------
from __future__ import print_function

import sys
import time
import datetime

#from urlparse import urlunsplit

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

from xml.dom.minidom import parseString
from fits_storage.utils import programs
from fits_storage import fits_storage_config as fsc
from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix
from gemini_obs_db.utils.gemini_metadata_utils import gemini_semester, previous_semester

from optparse import OptionParser

import requests

import json


__version__ = "0.1"

# ------------------------------------------------------------------------------
# fits URLs
prodfitsurl = 'https://archive.gemini.edu/ingest_programs'
# ------------------------------------------------------------------------------

def update_program_dbtable(url, pinfo):
    payload = list()
    for prog in pinfo:
        payload.append(prog)
        if len(payload) >= 20:
            req = requests.post(url, data=json.dumps(payload), cookies={'gemini_api_authorization': fsc.magic_api_cookie})
            req.raise_for_status()
            payload = list()
    if payload:
        req = requests.post(url, data=json.dumps(payload), cookies={'gemini_api_authorization': fsc.magic_api_cookie})
        req.raise_for_status()

# ------------------------------------------------------------------------------
# From http://swg.wikis-internal.gemini.edu/index.php/ODB_Browser
# ODB query service URL bits
ODB_URLS = {'gemini-north' : 'gnodb.hi.gemini.edu:8442',
            'gemini-south' : 'gsodb.cl.gemini.edu:8442'
           }
# ------------------------------------------------------------------------------


def netloc():
    """
    Site selection, North or South.

    Returns
    -------
    str : hostname and port to query for the ODB webservice
    """
    def get_site():
        local_site = "Unknown location."
        timezone = time.timezone / 3600
        if timezone == 10:
            local_site = 'gemini-north'
        elif timezone in [3, 4]:            # TZ -4 but +1hr DST is inconsistent
            local_site = 'gemini-south'
        else:
            raise RuntimeError(local_site)

        return local_site

    return ODB_URLS[get_site()]

odb_scheme = 'http'
odb_netloc = netloc()
odb_path   = 'odbbrowser/observations'
#odb_query  = 'programSemester={}'
odb_query  = 'programSemester='
odbq_parts = [odb_scheme, odb_netloc, odb_path, odb_query, None]


def do_semester(semester):
    """
    Retrieve all ODB program data for a given semester and update database

    This queries the ODB webservice for programs for the given semester.
    Then it updates the database accordingly.

    Parameters
    ----------
    semester : str
        Semester to query, such as 2020A
    """
    qrl = "%s://%s/%s/%s%s" % (odb_scheme, odb_netloc, odb_path, odb_query, semester)
    logger.info("Requesting ODB program metadata for semester %s", semester)
    logger.info("ODB URL %s", qrl)
    r = requests.get(qrl)
    pdata = r.text
    xdoc = parseString(pdata)
    pdata = programs.build_odbdata(programs.get_programs(xdoc))
    update_program_dbtable(prodfitsurl, pdata)
    logger.info("Semester %s appears to have completed sucessfully", semester)


def auto_semesters():
    """
    Calculate the current and previous semester and return.

    Determines the current and previous semester to assist
    in automatically running current information.

    Returns
    -------
    array of str : Returns current and previous semesters
    """
    # Return a list giving the current and previous semester
    today = datetime.datetime.now().date()
    this = gemini_semester(today)
    last = previous_semester(this)
    return [this, last]


# ------------------------------------------------------------------------------

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run as a background demon, do not generate stdout")
    parser.add_option("--semester", action="store", dest="semester", default=None, help="Semester to transfer data for. Omit for auto")
    options, args = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    if options.semester is None:
        logger.info("Will automatically select semesters")

    if options.semester:
        do_semester(options.semester)
    else:
        for s in auto_semesters():
            do_semester(s)
        
    sys.exit()
