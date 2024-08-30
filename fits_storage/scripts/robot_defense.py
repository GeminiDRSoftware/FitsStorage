#!/usr/bin/env python3

from argparse import ArgumentParser

from fits_storage.logger import logger, setdebug, setdemon

from fits_storage.db import sessionfactory
from fits_storage.server.robot_defense import get_ipprefix
from fits_storage.server.orm.ipprefix import IPPrefix

parser = ArgumentParser()
parser.add_argument("--debug", action="store_true", dest="debug",
                    help="Increase log level to debug")
parser.add_argument("--demon", action="store_true", dest="demon",
                    help="Run as a background demon, do not generate stdout")
parser.add_argument("--dryrun", action="store_true", dest="dryrun",
                    help="Do not actually block the prefixes")
parser.add_argument("--bgpapi", action="store", dest="bgpapi", default=None,
                    help="BGP API service to use. bgpview or arin. If not "
                         "specified, try bgpview and fall back to arin if it "
                         "fails.")
parser.add_argument('--ip', type=str,
                    help="Lookup and add prefixes for this IP address if prefix"
                         " for ip address is not alredy known. "
                         "Do not do log analysis.")
args = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(args.debug)
setdemon(args.demon)

session = sessionfactory()

if args.ip:
    ipp = get_ipprefix(session, args.ip, api=args.bgpapi, logger=logger)

    logger.info("Got prefix %s for address %s", ipp, args.ip)
    num = session.query(IPPrefix).filter(IPPrefix.asn == ipp.asn).count()
    logger.info("Database now contains %s entries for ASN %s", num, ipp.asn)
