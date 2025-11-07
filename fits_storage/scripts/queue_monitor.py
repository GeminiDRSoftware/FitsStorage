#! /usr/bin/env python3

from argparse import ArgumentParser
import datetime
import time

from fits_storage.logger import logger, setdebug, setdemon

from fits_storage.db import sessionfactory

from fits_storage.queues.orm.ingestqueueentry import IngestQueueEntry
from fits_storage.queues.orm.exportqueueentry import ExportQueueEntry
from fits_storage.queues.orm.fileopsqueueentry import FileopsQueueEntry
from fits_storage.queues.orm.calcachequeueentry import CalCacheQueueEntry
from fits_storage.queues.orm.previewqueueentry import PreviewQueueEntry
from fits_storage.queues.orm.reducequeentry import ReduceQueueEntry

from fits_storage.config import get_config
fsc = get_config()

parser = ArgumentParser()
parser.add_argument("--debug", action="store_true", dest="debug",
                    default=False, help="Increase log level to debug")

parser.add_argument("--demon", action="store_true", dest="demon",
                    default=False,
                    help="Run as background demon, do not generate stdout")
parser.add_argument("--minutes", action="store", dest="minutes", type=int,
                    default=60, help="Number of minutes between queue samples")

args = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(args.debug)
setdemon(args.demon)

# Announce startup
logger.info("***   queue_monitor.py - starting up at %s",
            datetime.datetime.now())

session = sessionfactory()

# Set up the list of queueentry ORM classes we are going to monitor.
# queues is a dict of dicts.
queues = {'Ingest': {'orm': IngestQueueEntry, 'inprogress': []},
          'Export': {'orm': ExportQueueEntry, 'inprogress': []},
          'FileOps': {'orm': FileopsQueueEntry, 'inprogress': []}}

if fsc.is_archive:
    queues['CalCache'] = {'orm': CalCacheQueueEntry, 'inprogress': []}
    queues['Preview'] = {'orm': PreviewQueueEntry, 'inprogress': []}
    queues['Reduce'] = {'orm': ReduceQueueEntry, 'inprogress': []}

# We don't need pidfile or signal handling, this isn't a queue service script.
while True:
    for queue in queues.keys():
        ormclass = queues[queue]['orm']
        oldinp = queues[queue]['inprogress']
        orms = session.query(ormclass).filter(ormclass.inprogress == True).all()

        # Were any of the items that are currently in progress, in progress last
        # time around the loop?
        newinp = []
        stuck = []
        logger.info('Queue %s has %d items in progress', queue, len(orms))
        for orm in orms:
            newinp.append(orm.id)
            if orm.id in oldinp:
                stuck.append(orm.id)
        if len(stuck):
            logger.critical("%s has %d items stuck in progress: %s",
                            queue, len(stuck), stuck)
        queues[queue]['inprogress'] = newinp

    time.sleep(args.minutes * 60)
