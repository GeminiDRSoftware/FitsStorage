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
# ------------------------------------------------------------------------------
# fits URLs
prodfitsurl = 'http://fits.cl.gemini.edu:8080/ingest_programs'
testfitsurl = 'http://sbffits-dev-lv1.cl.gemini.edu:8080/ingest_programs'
# ------------------------------------------------------------------------------
def get_programs(xdoc):
    programs = xdoc.getElementsByTagName("program")
    return programs

def get_investigators(program):
    """
    Parameters
    ----------
    program: <list>
        A list of dom elements for a program ID in the ODB data.

    Return
    ------
    tuple: (<str>, <str>); investigator names, piEmail. By definitiion, the
        *first* name in the investigator names string is the PI. 

    E.g., return

    ("auth_1, auth_2, auth_3", "auth_1@goo.edu")

    """
    investigatorNames = []
    for iname in program.getElementsByTagName('investigators'):
        for n in iname.getElementsByTagName('investigator'):
            if n.attributes['pi'].value == 'true':
                piEmail = n.getElementsByTagName('email')[0].childNodes[0].data
                piName = n.getElementsByTagName('name')[0].childNodes[0].data
                investigatorNames.insert(0, piName)
                continue
            name_actual = n.getElementsByTagName('name')[0].childNodes[0].data
            investigatorNames.append(name_actual)
        inames = ', '.join(f.encode('utf-8') for f in investigatorNames)

    return inames, piEmail

def get_obslog_comms(program):
    """
    Parameters
    ----------
    program: <list>
        A list of dom elements, one for each program in the ODB data.

    Return
    ------
    <list>: [ {}, {} , ... ], a list of dictionaries containing the datalabel
        and comments associated with that datalabel.

    E.g.,

    [ { "label": "GN-2012A-Q-114-34-004", 
        "comment": "Not applying more offsets, ... "},
      { ... },
      ...
    ]

    """
    logcomments = []
    observations = program.getElementsByTagName('observations')
    for obs in observations:
        for olog in obs.getElementsByTagName('obsLog'):
            for dset in olog.getElementsByTagName('dataset'):
                comments = []
                did = dset.getElementsByTagName('id')[0].childNodes[0].data
                for record in dset.getElementsByTagName('record'):
                    comments.append(record.childNodes[0].data)
                    comment_string = ", ".join(c.encode('utf-8') for c in comments)
                logcomments.append({"label": did, "comment": comment_string})
                comment_string = ''
    return logcomments

def get_reference(program):
    ref = program.getElementsByTagName('reference')
    return ref[0].childNodes[0].data

def get_title(program):
    ptitle = program.getElementsByTagName('title')
    return ptitle[0].childNodes[0].data

def get_contact(program):
    cse = program.getElementsByTagName('contactScientistEmail')
    return cse[0].childNodes[0].data

def get_abstract(program):
    stract = program.getElementsByTagName('abstrakt')
    try:
        abstract = stract[0].childNodes[0].data
    except IndexError:
        abstract = "No abstract"

    return abstract

def build_odbdata(programs):
    semester_data = []
    for program in programs:
        odb_data = {}
        odb_data['reference'] = get_reference(program)
        odb_data['title'] = get_title(program)
        odb_data['contactScientistEmail'] = get_contact(program)
        odb_data['abstrakt'] = get_abstract(program)
        odb_data['investigatorNames'], odb_data['piEmail'] = get_investigators(program)
        odb_data['observations'] = get_obslog_comms(program)
        semester_data.append(odb_data)
    return semester_data

def update_program_dbtable(pinfo):
    for prog in pinfo:
        send_to_server = json.dumps(prog)
        print()
        print(send_to_server)
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
    programs = get_programs(xdoc)
    pdata = build_odbdata(programs)
    update_program_dbtable(pdata)
    sys.exit()
