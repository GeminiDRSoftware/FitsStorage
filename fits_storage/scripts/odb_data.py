#!/usr/bin/env python
#                                                                            GOA
#                                                                    odb_data.py
# ------------------------------------------------------------------------------
from __future__ import print_function

import sys
import time
import urllib2

from urlparse import urlunsplit

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

from xml.dom.minidom import parseString
from fits_storage.utils import programs

import requests

__version__ = "0.1"
# ------------------------------------------------------------------------------
desc = """
Description:
  ODB to GOA metadata transfer. The command line accepts a Gemini semester
  identifier, queries the ODB for program information for all programs in that
  semester, extracts certain of the information, and passes it to the archive
  (fits) server via the ingest_programs service.

    E.g.,

    $ odb_data.py 2012A
    Requesting ODB program metadata for semester 2012A ...

"""

# ------------------------------------------------------------------------------
# fits URLs
prodfitsurl = 'http://fits.cl.gemini.edu:8080/ingest_programs'
testfitsurl = 'http://sbffits-dev-lv1.cl.gemini.edu:8080/ingest_programs'
# ------------------------------------------------------------------------------

def update_program_dbtable(url, pinfo):
    for prog in pinfo:
        req = requests.post(url, json=prog)
        req.raise_for_status()

def buildParser(version=__version__):
    """
    Parameters
    ----------
    version: <str>, defaulted optional version.

    Return
    ------
    <instance>, ArgumentParser instance

    """
    parser = ArgumentParser(description=desc, prog="odb_data", formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-v", "--version", action="version", version="%(prog)s v" + version)
    parser.add_argument('semester', nargs=1, default=None)
    return parser

def handle_clargs():
    parser = buildParser()
    return parser.parse_args()

# ------------------------------------------------------------------------------
# From http://swg.wikis-internal.gemini.edu/index.php/ODB_Browser
# ODB query service URL bits
ODB_URLS = {'gemini-north' : 'gnodb.hi.gemini.edu:8442',
            'gemini-south' : 'gsodb.cl.gemini.edu:8442'
           }
# ------------------------------------------------------------------------------
def netloc():
    """ Site selection, North or South """
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
odb_query  = 'programSemester={}'
odbq_parts = [odb_scheme, odb_netloc, odb_path, odb_query, None]
# ------------------------------------------------------------------------------

if __name__ == '__main__':
    args = handle_clargs()
    qrl = urlunsplit(odbq_parts).format(args.semester[0])
    print("Requesting ODB program metadata for semester {}".format(args.semester[0]))
    print("On URL {}".format(qrl))
    pdata = urllib2.urlopen(qrl).read()
    xdoc = parseString(pdata)
    pdata = programs.build_odbdata(programs.get_programs(xdoc))
    update_program_dbtable(prodfitsurl, pdata)
    sys.exit()
