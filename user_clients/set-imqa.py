#! /usr/bin/env python3

import datetime
import requests
import os

from argparse import ArgumentParser

from fits_storage.logger import logger, setdebug, setdemon

parser = ArgumentParser(prog='set-imqa.py.py',
                        description='Update QA states from im results')

parser.add_argument("--server", action="store", dest="server",
                    help="FitsStorage Server to use")
parser.add_argument("--dryrun", action="store_true",
                    dest="dryrun", help="Do not actually update headers")
parser.add_argument("--debug", action="store_true", dest="debug",
                    default=False, help="Increase log level to debug")
parser.add_argument("--demon", action="store_true", dest="demon",
                    default=False, help="No not log to stdout")
parser.add_argument("--selection", action="store", dest="selection",
                    help="selection string for files to update")
parser.add_argument("--fail", action="store_true", dest="qafail",
                    help="set to Fail")
parser.add_argument("--pass", action="store_true", dest="qapass",
                    help="set to Pass")
options = parser.parse_args()

# Logging level to debug?
setdebug(options.debug)
setdemon(options.demon)

# Announce startup
logger.info(f"***   set-imqa.py - starting up at {datetime.datetime.now()}")

# Query server to get filename
url = f"http://{options.server}/jsonqastate/present/{options.selection}"
r = requests.get(url)
if r.status_code != 200:
    logger.error(f"Bad http status {r.status_code} for {url}. Exiting")
    exit(1)
jqas = r.json()

if len(jqas):
    logger.info(f"Found {len(jqas)} files for {options.selection}")
else:
    logger.error(f"Got no files for {options.selection}. Exiting")
    exit(2)

filenames = []
for jqa in jqas:
    filenames.append(jqa['filename'])
    logger.info(f"{jqa['filename']} {jqa['data_label']} {jqa['qa_state']}")

if options.dryrun:
    logger.info("Dryrun - stopping now")
    exit(0)

cookie = os.environ.get('GEMINI_API_AUTHORIZATION', None)
if cookie is None:
    logger.error("GEMINI_API_AUTHORIZATION not set. Exiting")
    exit(3)

if options.qapass:
    qastate = 'Pass'
elif options.qafail:
    qastate = 'Fail'
else:
    logger.error("No QA state provided, exiting")
    exit(4)

# Build the update request
request = []

for fn in filenames:
    values = {'qastate': qastate, 'generic': [('IMQASET', qastate)]}
    requestdict = {'filename': fn, 'values': values, 'reject_new': False}
    request.append(requestdict)

url = f"http://{options.server}/update_headers"
logger.info(f'POSTing request to {url}...')
cookies = {'gemini_api_authorization': cookie}
r = requests.post(url, cookies=cookies, json=request)
if r.status_code != 200:
    logger.error(f"Got bad status code {r.status_code}. Exiting")
    exit(5)
else:
    logger.debug("Got HTTPOK status.")

response = r.json()
if response:
    logger.info(f"Got {len(response)} responses")
else:
    logger.error(f"Got bad response: {response}")
for item in response:
    logger.info(f"{item['id']}: {item['result']}")

logger.info(f"***   set-imqa.py - exiting normally at {datetime.datetime.now()}")
