#!/usr/bin/env python
#                                                                            GOA
#                                                                    odb_data.py
# ------------------------------------------------------------------------------
from __future__ import print_function

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
import sys
import json
import time
import urllib2

from urlparse import urlunsplit

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

from xml.dom.minidom import parseString
from fits_storage.utils import programs

# ------------------------------------------------------------------------------
# fits URLs
prodfitsurl = 'http://fits.cl.gemini.edu:8080/ingest_programs'
testfitsurl = 'http://sbffits-dev-lv1.cl.gemini.edu:8080/ingest_programs'
# ------------------------------------------------------------------------------

def update_program_dbtable(pinfo):
    for prog in pinfo:
        print()
        json.dump(prog, sys.stdout)
        print()
        #req = urllib2.Request(url=testfitsurl, data=send_to_server)
        #f = urllib2.urlopen(req)
    return

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
    args = parser.parse_args()
    return args

# ------------------------------------------------------------------------------
# From http://swg.wikis-internal.gemini.edu/index.php/ODB_Browser
# ODB query service URL bits
GeminiNorthTest = 'gnodbtest2.hi.gemini.edu:8442'
GeminiSouthTest = 'gsodbtest2.cl.gemini.edu:8442'
ODB_URLS = { 'gemini-north' : 'gnodb.hi.gemini.edu:8442',
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

    odb_netloc = ODB_URLS[get_site()]
    return odb_netloc

odb_scheme = 'http'
odb_netloc = netloc()
odb_netloc = GeminiNorthTest                      # @TODO remove for production.
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
    update_program_dbtable(pdata)
    sys.exit()
