#!/usr/bin/env python

"""
This script pulls 
"""

import json
from contextlib import closing
from importlib import import_module
# Note: use the proper MySQL driver, depending on what's available
#       mysql.connector is pure Python and can be installed without
#       the extra requirement of MySQL C Client libs
import mysql.connector as mysql
# import MySQLdb as mysql
import requests

from fits_storage import fits_storage_config as fsc

DSN = dict(
  host=fsc.pubdb_host,
  user=fsc.pubdb_username,
  db=fsc.pubdb_dbname
)

def process_publication(row):
    """
    Takes a database row representing a publication and normalizes/removes
    certain values, preparing them for ingestion by the web site.

    Returns a dictionary
    """
    copy = dict(row)
    print(("Processing {0}".format(copy.get('bibcode'))))
    for (key, value) in list(copy.items()):
        if isinstance(value, str):
            value = value.strip()
        if value is None or value == '':
            del copy[key]
    if 'program_id' in copy:
        copy['programs'] = tuple(p.strip() for p in copy['program_id'].split(','))
        del copy['program_id']
    return copy

ALL_PUBS_SELECT = """
SELECT author, title, year, journal, telescope, instrument, country, bibcode,
       wavelength, mode, gstaff, gsa, golden, too, partner, program_id,
       volume, page
FROM publication
"""

try:
    with closing(mysql.connect(**DSN)) as conn:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(ALL_PUBS_SELECT)
        payload = []
        for row in cursor:
            processed = process_publication(row)
            payload.append(processed)
        result = requests.post(fsc.pubdb_remote, json={'single': False, 'payload': payload},
                               cookies={'gemini_api_authorization': fsc.magic_api_cookie})
        result.raise_for_status()
except requests.HTTPError as exception:
    print(exception)
