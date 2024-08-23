#!/usr/bin/env python3

from argparse import ArgumentParser
from datetime import datetime

from fits_storage.logger import logger, setdebug, setdemon

from fits_storage.db import session_scope

from fits_storage.core.orm.header import Header
from fits_storage.core.orm.diskfile import DiskFile

from fits_storage.core.orm.footprint import Footprint, footprints

from fits_storage.core.geometryhacks import add_footprint

from sqlalchemy import desc

parser = ArgumentParser(
    description="This script is used to rebuild the footprints table. "
                "It is needed after improvements or bugfixes that affect"
                "calculation of footprints. You will need to run "
                "rebuild_stdstarobs.py after running rebuild_footprints.py")
parser.add_argument("--debug", action="store_true", dest="debug",
                    help="Increase log level to debug")
parser.add_argument("--demon", action="store_true", dest="demon",
                    help="Run as a background demon, do not generate stdout")

args = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(args.debug)
setdemon(args.demon)

# Announce startup
logger.info("***   rebuild_footprints.py - starting up at %s", datetime.now())

with session_scope() as session:
    # First up, we need to get a list of Headers that we're going to process.
    query = session.query(Header.id).join(DiskFile)
    # We need access to the file so can only do present files.
    query = query.filter(DiskFile.present == True)
    query = query.order_by(desc(Header.ut_datetime))

    hids = query.all()
    n = len(hids)
    logger.info("Got %d headers to process", n)

    i = 0
    for hid in hids:
        i += 1
        try:
            header = session.query(Header).filter(Header.id == hid[0]).one()
            logger.info("Processing %s (%d / %d)",
                        header.diskfile.filename, i, n)

            ad = header.diskfile.get_ad_object

            for label, fp in footprints(ad, logger).items():
                footprint = Footprint(header)
                footprint.extension = label
                session.add(footprint)
                session.flush()
                add_footprint(session, footprint.id, fp)
            session.commit()
        except:
            logger.info("Failed on filename %s", header.diskfile.filename)

logger.info("***   rebuild_footprints.py - exiting up at %s", datetime.now())
