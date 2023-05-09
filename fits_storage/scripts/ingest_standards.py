#! /usr/bin/env python3

import os
import datetime

from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.server.ingest_standards import ingest_standards

from fits_storage.db import session_scope

from fits_storage.config import get_config
fsc = get_config()

"""
Read in the list of standards from `standards.txt` and add them to the database.
"""

if __name__ == "__main__":

    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--file", action="store", type="string",
                      dest="filename", help="Standards text filename")
    parser.add_option("--debug", action="store_true", dest="debug",
                      default=False, help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon",
                      default=False, help="Run in background mode")

    (options, args) = parser.parse_args()

    # Logging level to debug?
    setdebug(options.debug)
    setdemon(options.demon)

    # Announce startup
    logger.info("*********  ingest_standards.py - starting up at %s" %
                datetime.datetime.now())
    logger.debug("Config files used: %s", ', '.join(fsc.configfiles_used))

    if options.filename:
        filename = options.filename
    else:
        filename = os.path.join(fsc.aux_data_dir, "standards.txt")
    logger.info("Reading from file: %s" % filename)

    with session_scope() as session:
        ingest_standards(session, filename, logger=logger)

    logger.info("*** ingest_standards exiting normally at %s" %
                datetime.datetime.now())
