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
  passwd=fsc.pubdb_password,
  db=fsc.pubdb_dbname
)

def process_publication(row):
    copy = dict(row)
    print("Processing {0}".format(copy.get('bibcode')))
    for (key, value) in list(copy.items()):
        if isinstance(value, (str, unicode)):
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

with closing(mysql.connect(**DSN)) as conn:
    cursor = conn.cursor(dictionary=True)
    cursor.execute(ALL_PUBS_SELECT)
    payload = []
    for row in cursor:
        processed = process_publication(row)
        payload.append(processed)
    requests.post(fsc.pubdb_remote, json={'single': False, 'payload': payload})
