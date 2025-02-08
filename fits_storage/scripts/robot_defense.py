#!/usr/bin/env python3

from sqlalchemy.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.sql import func

from argparse import ArgumentParser

from fits_storage.logger import logger, setdebug, setdemon

from fits_storage.db import sessionfactory
from fits_storage.server.prefix_helpers import get_ipprefix
from fits_storage.server.orm.ipprefix import IPPrefix
from fits_storage.server.orm.usagelog import UsageLog
from fits_storage.server.orm.usagelog_analysis import UsageLogAnalysis

from fits_storage.gemini_metadata_utils.datestimes import gemini_date, \
    gemini_daterange, get_time_period

from fits_storage.config import get_config
fsc = get_config()

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
                    help='Date range to operate on for --analyse or --update. '
                         'Can be a YYYYMMDD date, today or yesterday, or a '
                         'yyyymmdd-YYYYMMDD daterange')
parser.add_argument('--analyse', action='store_true', dest='analyse',
                    help='Perform usagelog analysis, see --date')
parser.add_argument('--force', action="store_true", dest='force', default=False,
                    help="Do not skip re-analysis of previously analysed log "
                         "entries")
parser.add_argument('--update', action='store_true', dest='update',
                    help='Update the badness scores and deny attributes in '
                         'the ipprefix table. See --date')
parser.add_argument('--update-reset', action='store', dest='update_reset',
                    help='Reset the ippprefix attributes before updating them. '
                         'Without this, updates are cumulative.')
args = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(args.debug)
setdemon(args.demon)

# If given a date argument, parse it, and set start and end
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
            logger.error("Could not parse date given: %s", args.date)
            exit(1)
        start, end = get_time_period(d)
else:
    start, end = None, None

session = sessionfactory()

if args.ip:
    ipp = get_ipprefix(session, args.ip, api=args.bgpapi, logger=logger)

    logger.info("Got prefix %s for address %s", ipp, args.ip)
    num = session.query(IPPrefix).filter(IPPrefix.asn == ipp.asn).count()
    logger.info("Database now contains %s entries for ASN %s", num, ipp.asn)
    exit(0)

if args.analyse and not args.date:
    logger.error("Must specify --date with --analyze")
    exit(1)

if args.update and not args.date:
    logger.error("Must specify --date with --update")
    exit(1)

if args.analyse:
    logger.info("Analysing log for timestamp range: %s - %s", start, end)

    # Note, don't filter on http_status here: a) don't filter out the redirects
    # as the searchform url normalization would clean up the duplicates that
    # we flag on, b) don't filter 404s as we flag those as bad requests too.
    # c) Don't filter denied requests as we should keep updating the badness
    # score for those prefixes if they keep attempting access.
    query = session.query(UsageLog.id).filter(UsageLog.utdatetime >= start)\
        .filter(UsageLog.utdatetime < end)
    ulids = query.all()
    n = len(ulids)
    logger.info("%d UsageLog entries to check", n)

    previous = 0
    analysed = 0
    i = 0
    for ulidl in ulids:
        ulid = ulidl[0]
        i += 1
        if i % 1000 == 0:
            logger.info("Analysing... (%d / %d)", i, n)

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

if args.update:
    # We avoid walking through the log entries here, we can have the database
    # do the leg work for efficiency. We want a list of
    # (ipprefix_id, sum(total_score)) pairs from the UsageLogAnalysis entries.
    # We can do this with a single select prefix_id, sum(...) ... group by query

    query = session.query(UsageLogAnalysis.prefix_id,
                          func.sum(UsageLogAnalysis.total_score))\
        .select_from(UsageLog).join(UsageLogAnalysis) \
        .filter(UsageLog.utdatetime >= start) \
        .filter(UsageLog.utdatetime < end) \
        .filter(~UsageLogAnalysis.prefix_id.is_(None)) \
        .group_by(UsageLogAnalysis.prefix_id)

    pairs = query.all()
    logger.info("Found %d Prefixes to update", len(pairs))

    # We now walk through that list, updating the IPPrefix entries.
    for (prefixid, sum_score) in pairs:
        ipp = session.query(IPPrefix).get(prefixid)
        if args.update_reset:
            logger.debug("Resetting score and deny for prefix %s", ipp.prefix)
            ipp.badness = sum_score
            ipp.deny = False
        else:
            ipp.badness += sum_score
        logger.debug("Badness score for prefix %s is now %d",
                     ipp.prefix, ipp.badness)
        if ipp.badness > fsc.robot_badness_threshold:
            if ipp.allow:
                logger.warning("Prefix %s would be DENIED (badness %d > %d), "
                               "but has ALLOW flag set", ipp.prefix,
                               ipp.badness, fsc.robot_badness_threshold)
            elif ipp.deny:
                logger.info("Updating Badness on already denied prefix %s: %d",
                            ipp.prefix, ipp.badness)
            else:
                logger.info("Setting DENY flag for prefix %s: Badness %d > %d",
                            ipp.prefix, ipp.badness,
                            fsc.robot_badness_threshold)
                ipp.deny = True
        session.commit()
