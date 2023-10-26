#! /usr/bin/env python3

import datetime

from fits_storage.config import get_config
fsc = get_config()

from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix

from fits_storage.db import session_scope
from fits_storage.core.orm.diskfile import DiskFile

from fits_storage.queues.queue.reducequeue import ReduceQueue


if __name__ == "__main__":
    # Option Parsing
    from argparse import ArgumentParser
    # ------------------------------------------------------------------------------
    parser = ArgumentParser()

    parser.add_argument("--debug", action="store_true", dest="debug",
                        help="Increase log level to debug")

    parser.add_argument("--demon", action="store_true", dest="demon",
                        help="Run in the background, do not generate stdout")

    parser.add_argument("--filenames", action="extend", type=str,
                        dest="filenames", default=[], nargs='+',
                        help="Add this comma separated list of filenames as a "
                             "single entry to the queue")

    parser.add_argument("--listfile", action="store", type=str, default=None,
                        help="Filename of a file containing a list of files to"
                             "add to the queue as a single entry")

    parser.add_argument("--logsuffix", action="store", type=str,
                        dest="logsuffix", default=None,
                        help="Extra suffix to add on logfile")

    options = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Check Log Suffix
    if options.logsuffix:
        setlogfilesuffix(options.logsuffix)

    # Announce startup
    logger.info("*** add_to_reduce_queue.py - starting up at {}"
                .format(datetime.datetime.now()))
    logger.debug("Config files used: %s", ', '.join(fsc.configfiles_used))

    if options.filenames:
        # Just add a list of filename
        logger.info("Adding single entry list of filenames: %s",
                    options.filenames)
        files = options.filenames

    elif options.listfile:
        # Get list of files from list file
        logger.info("Adding files from list file: %s" % options.listfile)
        files = []
        with open(options.listfile) as f:
            for line in f:
                files.append(line.strip())

    else:
        logger.info("No list of filenames was provided.")
        files = []

    with session_scope() as session:
        # Check that all the filenames given are valid and ensure they end in
        # .fits
        validfiles = []
        for filename in files:
            if filename.endswith('.fits.bz2'):
                filename = filename.removesuffix('.bz2')
            elif filename.endswith('.fits'):
                pass
            else:
                filename += '.fits'

            possible_filenames = [filename, filename+'.bz2']

            query = session.query(DiskFile).filter(DiskFile.present == True)\
                .filter(DiskFile.filename.in_(possible_filenames))

            if query.count() == 0:
                logger.error("Filename %s not found in database, not adding "
                             "this list to the queue", filename)
            else:
                validfiles.append(filename)

        logger.debug("List of validated files: %s", validfiles)

        if len(validfiles):
            rq = ReduceQueue(session, logger=logger)
            logger.info("Queuing a batch of %s files for reduce, starting with %s",
                        len(validfiles), validfiles[0])
            rq.add(validfiles)
        else:
            logger.error("No valid files to add")

    logger.info("*** add_to_reducequeue.py exiting normally at %s",
                datetime.datetime.now())
