#!/usr/bin/env python3

from sqlalchemy.exc import NoResultFound, MultipleResultsFound
from argparse import ArgumentParser

from fits_storage.logger import logger, setdebug, setdemon

from fits_storage.db import sessionfactory
from fits_storage.server.prefix_helpers import get_ipprefix
from fits_storage.server.orm.ipprefix import IPPrefix
from fits_storage.server.orm.usagelog import UsageLog
from fits_storage.server.orm.usagelog_analysis import UsageLogAnalysis

from fits_storage.gemini_metadata_utils.datestimes import gemini_date, \
    gemini_daterange, get_time_period

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
parser.add_argument('--date', action="store", dest='date', default=None,
                    help='Analyse all usagelog entries for this YYYYMMDD date, '
                         'or this yyyymmdd-YYYYMMDD daterange')
parser.add_argument('--force', action="store_true", dest='force', default=False,
                    help="Do not skip re-analysis of previously analysed log "
                         "entries")
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
    exit(0)

if args.date:
    if '-' in args.date:
        try:
            dstart, dend = gemini_daterange(args.date, as_dates=True)
            start, end = get_time_period(dstart, end=dend)
        except Exception:
            logger.error("Could not parse date range given: %s", args.date)
            exit(1)
    else:
        d = gemini_date(args.date, as_date=True)
        if d is None:
            logger.error("Could not parse date gived: %s", args.date)
            exit(1)
        start, end = get_time_period(d)
    logger.info("Analysing log for timestamp range: %s - %s", start, end)

    query = session.query(UsageLog.id).filter(UsageLog.utdatetime >= start)\
        .filter(UsageLog.utdatetime < end)
    ulids = query.all()
    logger.info("%d UsageLog entries to check", len(ulids))

    previous = 0
    analysed = 0
    for ulidl in ulids:
        ulid = ulidl[0]

        create_entry = False
        try:
            ula = session.query(UsageLogAnalysis)\
                .filter(UsageLogAnalysis.usagelog_id == ulid).one()
            previous += 1
        except MultipleResultsFound:
            logger.error("Multiple UsageLogAnalysis entries found for "
                         "usagelog_id %d. This should not happen.", ulid)
            continue
        except NoResultFound:
            create_entry = True
            ula = None
        if args.force and ula is not None:
            session.delete(ula)
            session.commit()
            create_entry = True

        if create_entry:
            ula = UsageLogAnalysis(ulid)
            session.add(ula)
            session.commit()
            ula.analyse(api=args.bgpapi, logger=logger)
            session.commit()
            analysed += 1
    logger.info("%s UsageLog entries previously analysed", previous)
    logger.info("%s UsageLog entries analysed", analysed)
