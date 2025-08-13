#! /usr/bin/env python3

import datetime
from argparse import ArgumentParser

from sqlalchemy import select
from sqlalchemy.exc import NoResultFound, MultipleResultsFound

from fits_storage.logger import logger, setdebug, setdemon

from fits_storage.db import sessionfactory

from fits_storage.server.orm.monitoring import Monitoring
from fits_storage.core.orm.header import Header, DiskFile

from fits_storage.config import get_config
fsc = get_config()


parser = ArgumentParser(prog='refresh_monitoring_headerids.py',
                        description='Refresh the header_id value of monitoring'
                                    'entries to a present entry')
parser.add_argument("--filepre", action="store", dest="filepre",
                    help="Update Monitoring entries with this filename prefix")
parser.add_argument("--dlpre", action="store", dest="dlpre",
                    help="Update Monitoring entries with this data_label prefix")
parser.add_argument("--ptag", action="store", dest="ptag",
                    help="Update Monitoring entries with this processing tag")
parser.add_argument("--all", action="store_true", dest="all",
                    help="Update all Monitoring entries.")
parser.add_argument("--debug", action="store_true", dest="debug",
                    default=False, help="Increase log level to debug")
parser.add_argument("--demon", action="store_true", dest="demon", default=False,
                    help="Run as background demon, do not generate stdout")
options = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Announce startup
logger.info("***   refresh_monitoring_headerids.py - starting up at %s",
            datetime.datetime.now())
logger.debug("Config files used: %s", ', '.join(fsc.configfiles_used))

if not any([options.all, options.filepre, options.dlpre, options.ptag]):
    logger.error("Must specify at least one monitoring selection options")
    exit(1)

session = sessionfactory()

statement = select(Monitoring)
if options.filepre:
    statement = statement.where(Monitoring.filename.startswith(options.filepre))
if options.dlpre:
    statement = statement.where(Monitoring.data_label.startswith(options.dlpre))
if options.ptag:
    statement = statement.where(Monitoring.processing_tag == options.ptag)

num = 0
for mon in session.scalars(statement):
    if mon.header.diskfile.present:
        logger.debug(f"Monitoring id {mon.id} diskfile is present")
        continue
    # Find new header id to replace old header id, by diskfile filename and path
    filename = mon.header.diskfile.filename
    path = mon.header.diskfile.path

    find_header_statement = select(Header).join(Header.diskfile) \
        .where(DiskFile.filename == filename) \
        .where(DiskFile.path == path).where(DiskFile.present)

    try:
        new_header = session.execute(find_header_statement).one()
    except NoResultFound:
        logger.warning(f"No present diskfile found for {path}/{filename}")
        continue
    except MultipleResultsFound:
        logger.warning(f"Multiple present diskfiles found for {path}/{filename}")
        continue

    logger.info(f"Replacing header id {mon.header_id} with {new_header.id} "
                f"for {mon.id} - {path}/{filename}")
    mon.header_id = new_header.id
    num += 1

logger.info(f"Updated {num} header IDs.")
if num:
    logger.info("Commiting session...")
    session.commit()

logger.info("***   refresh_monitoring_headerids.py - exiting normally at %s",
            datetime.datetime.now())