#! /usr/bin/env python3

import datetime
import subprocess

from fits_storage.config import get_config
from fits_storage.logger import logger, setdebug, setdemon

from optparse import OptionParser

fsc = get_config()

"""
Script to vacuum the Postgres database.
"""
if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("--debug", action="store_true", dest="debug",
                      help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon",
                      help="Run as a background demon, do not generate stdout")

    (options, args) = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Announce startup
    logger.info("*********  database_vacuum.py - starting up at %s",
                datetime.datetime.now())

    command = ["/usr/bin/vacuumdb",
               "--verbose",
               "--analyze",
               "--dbname", fsc.fits_dbname]

    logger.info("Executing vacuumdb")

    sp = subprocess.Popen(command, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)

    (stdoutstring, stderrstring) = sp.communicate()

    logger.info(stderrstring.decode('UTF-8', "replace"))

    logger.info(stdoutstring.decode('UTF-8', "replace"))

    logger.info("-- Finished, Exiting at %s", datetime.datetime.now())
