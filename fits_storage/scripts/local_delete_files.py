#!/usr/bin/env python3

import os
import hashlib
import sys
import datetime
from optparse import OptionParser

from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.server.tapeutils import FileOnTapeHelper

from fits_storage.config import get_config
fsc = get_config()

# Option Parsing
parser = OptionParser()
parser.add_option("--tapeserver", action="store", type="string",
                  dest="tapeserver", default=fsc.tape_server,
                  help="FitsStorage Tape server to check the files are on tape")
parser.add_option("--file-pre", action="store", type="string", dest="filepre",
                  default='', help="File prefix to operate on, eg N200812 etc")
parser.add_option("--mintapes", action="store", type="int", dest="mintapes",
                  default=2, help="Minimum number of tapes file must be on to "
                                  "be eligible for deletion")
parser.add_option("--dir", action="store", type="string", dest="dir",
                  help="Directory to operate in")
parser.add_option("--nomd5", action="store_true", dest="nomd5",
                  help="Do not check md5, match on filename only")
parser.add_option("--dryrun", action="store_true", dest="dryrun",
                  help="Dry Run - do not actually do anything")
parser.add_option("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon",
                  help="Run as a background demon, do not generate stdout")
options, args = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Announce startup
logger.info("***   delete_files.py - starting up at %s",
            datetime.datetime.now())

logger.error("This script hasn't been tested properly since the great refactor"
             "of 2023. Please test before using...")
sys.exit(1)

if options.dir:
    os.chdir(options.dir)
else:
    logger.error("Must specify a dir to work in")
    sys.exit(1)

thelist = []
for fn in os.listdir('.'):
    if fn.startswith(options.filepre):
        thelist.append(fn)

logger.info("Files to consider: %s", thelist)

# We use the FileOnTapeHelper class here which provides caching..
foth = FileOnTapeHelper(tapeserver=options.tapeserver)

if options.filepre:
    logger.info("Pre-populating tape server results cache from filepre")
    foth.populate_cache(options.filepre)

for filename in thelist:
    if not os.path.isfile(filename):
        logger.error("%s is not a regular file - skipping", filename)
        continue
    if options.nomd5:
        logger.debug("Skipping MD5 check")
        filemd5 = None
    else:
        m = hashlib.md5()
        block = 1000000  # 1 MB
        with open(filename, 'rb') as f:
            while True:
                data = f.read(block)
                if not data:
                    break
                m.update(data)
        filemd5 = m.hexdigest()

    logger.info("Considering %s - %s", (filename, filemd5))

    # Check if it's on tape.
    tape_ids = foth.check_file(filename, filemd5)
    if len(tape_ids) < options.mintapes:
        logger.info("File %s is only on %d tapes (%s), not deleting",
                    (filename, len(tape_ids), str(tape_ids)))
        continue

    if options.dryrun:
        logger.info("Dryrun: not actually deleting file %s", filename)
    else:
        try:
            logger.info("Deleting file %s", filename)
            os.unlink(filename)
        except Exception:
            logger.error("Could not delete %s", filename, exc_info=True)
