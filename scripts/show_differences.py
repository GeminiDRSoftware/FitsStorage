#!/usr/bin/env python

"""Populates Redis with files to be processed by the parallel resolver

Usage:
  show_differences.py <filename>
  show_differences.py (-h | --help)
  show_differences.py --version

Options:
  -h --help               Show this screen.
  --version               Shows the program version and quits
"""

from docopt import docopt

import sys

import orm
from orm.resolve_versions import Version
import pyfits as pf
import bz2

from logger import logger, setdebug, setdemon

arguments = docopt(__doc__, version='Populat Redis 0.1')

setdebug(False)
setdemon(False)

# Annouce startup
logger.info("*********    show_differences.py - starting up")

fname = arguments['<filename>']
with orm.sessionfactory().no_autoflush as sess:
    paths = [x[0] for x in sess.query(Version.fullpath).filter(Version.filename == fname)]
    if not paths:
        logger.info("Found no instances of {0}".format(fname))
        sys.exit(0)

    logger.info("Found the following instances")
    for path in paths:
        logger.info(" - {0}".format(path))

    p_and_f = [(x, pf.open(bz2.BZ2File(x))) for x in paths]
    for ((afp, aobj), (bfp, bobj)) in zip(p_and_f[:-1], p_and_f[1:]):
        logger.info("-------------------------------------------------------------------------")
        logger.info("Differences between:")
        logger.info("  - {0}".format(afp))
        logger.info("  - {0}".format(bfp))

        for (ha, hb) in zip(aobj, bobj):
            print (pf.HDUDiff(ha, hb).report())
