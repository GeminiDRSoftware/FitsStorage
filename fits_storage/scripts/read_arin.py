#! /usr/bin/env python3

from optparse import OptionParser
from datetime import datetime
import ipaddress
import requests
from sqlalchemy import select, desc

from fits_storage.db import sessionfactory
from fits_storage.server.orm.arin import ArinAsn, ArinNetwork, ArinPrefix

from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.config import get_config


fsc = get_config()

parser = OptionParser()
parser.add_option("--fetchall", action="store_true",
                  help="Fetch all registry data from arin")
parser.add_option("--read", action="store", dest="read",
                  help="Read this arin delegated file")
parser.add_option("--prefixes", action="store_true",
                  help="Generate Prefixes")

parser.add_option("--debug", action="store_true", dest="debug", default=False,
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False,
                  help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

logger.info("*** read_arin.py starting up at %s", datetime.now())

session = sessionfactory()

def add_from_fp(fp):
    n = 0
    for line in fp:
        if isinstance(line, bytes):
            line = line.decode('ascii')
        line = line.strip()
        if line[0] == '#':
            # Comment line, don't increment n
            continue
        n += 1
        # Skip first 4 lines
        if n < 5:
            logger.debug(f"Skipping line {n}: {line}")
            continue
        things = str(line).split('|')
        try:
            type = things[2]
        except IndexError:
            print(f'Index Error on {things}')
            raise

        if type == 'asn':
            asn = ArinAsn(things)
            session.add(asn)
        elif type == 'ipv4':
            net = ArinNetwork(things)
            session.add(net)
        session.commit()
    return n

if options.read:
    logger.info(f'Reading from {options.read}')
    with open(options.read, 'r', newline='') as fp:
        n = add_from_fp(fp)
    logger.info(f'Read {n} Entries')

if options.fetchall:
    baseurl = 'https://ftp.arin.net/pub/stats'
    regs = ['arin', 'apnic', 'lacnic', 'afrinic', 'ripencc']
    for reg in regs:
        extended = 'extended-' if reg == 'arin' else ''
        url = f'{baseurl}/{reg}/delegated-{reg}-{extended}latest'
        r = requests.get(url, stream=True)
        if r.status_code == 200:
            logger.info(f'Reading from {url}')
            n = add_from_fp(r.iter_lines())
            logger.info(f'Read {n} Entries')
        else:
            logger.error(f'Got status code {r.status_code} for {url}')

if options.prefixes:
    stmt = select(ArinNetwork)
    n = 0
    for net in session.scalars(stmt):
        f = ipaddress.ip_address(net.ipstart)
        l = ipaddress.ip_address(net.ipend)
        for cidr in ipaddress.summarize_address_range(f, l):
            prefix = ArinPrefix()
            prefix.prefix = str(cidr)
            prefix.arinnetwork_id = net.id
            session.add(prefix)
            n += 1
    session.commit()
    logger.info(f"Processed {n} CIDRs")

logger.info("*** read_arin.py exiting normally at %s", datetime.now())
