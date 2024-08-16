#!/usr/bin/env python3

from argparse import ArgumentParser
from datetime import datetime

from fits_storage.logger import logger, setdebug, setdemon

from fits_storage.db import session_scope

from fits_storage.core.orm.footprint import Header
from fits_storage.core.orm.diskfile import DiskFile

from fits_storage.core.geometryhacks import do_std_obs

parser = ArgumentParser(
    description="This script is used to rebuild the standardstarobs table "
                "which associates standard stars with footprints. It is needed "
                "after the standard star table is updated. Depending on the "
                "updates you made, you may have to completely clear the "
                "standardstarobs table and rebuild it from scratch")

parser.add_argument("--debug", action="store_true", dest="debug",
                    help="Increase log level to debug")
parser.add_argument("--demon", action="store_true", dest="demon",
                    help="Run as a background demon, do not generate stdout")
parser.add_argument("--canonical", action="store_true", dest="canonical",
                    help="Only add entries for footprints from canonical "
                         "diskfiles")
args = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(args.debug)
setdemon(args.demon)

# Announce startup
logger.info("***   rebuild_stdstarobs.py - starting up at %s", datetime.now())

with session_scope() as session:
    # First up, we need to get a list of header_ids that we're going to process.
    query = session.query(Header.id)
    if args.canonical:
        query = query.join(DiskFile).filter(DiskFile.canonical == True)
    query = query.filter(Header.spectroscopy == False)

    hids = query.all()
    # Note, because we queried for Header.id, hids will be a list of tuples,
    # where each tuple just contains one element which is the header id.
    n = len(hids)
    logger.info("Got %d header ids to process", n)

    i = 0
    for thid in hids:
        hid = thid[0]
        i += 1

        do_std_obs(session, hid, commit=False)
        if i % 1000 == 0:
            logger.info("Processed hid %d (%d / %d)", hid, i, n)
            session.commit()

    logger.info("Processed hid %d (%d / %d)", hid, i, n)
    session.commit()