#!/usr/bin/env python3

import sys
from optparse import OptionParser

from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.server.tapeutils import TapeDrive
from fits_storage.config import get_config

# Option Parsing
parser = OptionParser()
parser.add_option("--read", action="store_true", dest="read",
                  help="Read the label from the tape in the drive")
parser.add_option("--label", action="store", dest="label",
                  help="Write the label to the tape in the drive. This will "
                       "write to the start of the tape, making any other "
                       "data on the tape inaccessible")
parser.add_option("--tapedrive", action="store", dest="tapedrive",
                  help="The tapedrive device to use")
parser.add_option("--force", action="store_true", dest="force",
                  help="Normally, --label will refuse to label a tape that "
                       "already contains a tapelabel. "
                       "This option forces it to do so.")

options, args = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

fsc = get_config()

if not (options.read or options.label):
    logger.error("You must supply either the --read or the --label option")
    sys.exit(1)

logger.error("This script hasn't been tested properly since the great refactor"
             " of 2023. Please test before using")
sys.exit(0)

td = TapeDrive(options.tapedrive, fsc.fits_tape_scratchdir)

if options.read:
    logger.info(td.readlabel(fail=False))
    sys.exit(0)


if options.label:
    oldlabel = td.readlabel(fail=False)
    if oldlabel:
        logger.warning("This tape already has a FitsStorage tape label")
        logger.info("Current label is: %s", oldlabel)
        if options.force:
            logger.info("--force specified: will overwrite")
            logger.info("Writing new tape label: %s", options.label)
            td.writelabel(options.label)
            sys.exit(0)
        else:
            logger.info("To overwrite, use the --force option")
            sys.exit(1)
    else:
        logger.info("Writing tape label: %s", options.label)
        td.writelabel(options.label)
        sys.exit(0)
