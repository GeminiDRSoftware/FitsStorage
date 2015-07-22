#!/usr/bin/env python

"""Populates Redis with files to be processed by the parallel resolver

Usage:
  show_differences.py [-p] <filename>
  show_differences.py (-h | --help)
  show_differences.py --version

Options:
  -p, --permissive  Don't panic on horrible headers
  -h --help         Show this screen.
  --version         Shows the program version and quits
"""

from docopt import docopt

import sys

import orm
from fits_storage.orm.resolve_versions import Version
import pyfits as pf
import bz2
from itertools import combinations

from fits_storage.logger import logger, setdebug, setdemon

arguments = docopt(__doc__, version='Populat Redis 0.1')

setdebug(False)
setdemon(False)

# Annouce startup
logger.info("*********    show_differences.py - starting up")

fname = arguments['<filename>']
verifyopt = ("silentfix+warn" if arguments['--permissive'] else "silentfix+exception")
with orm.sessionfactory().no_autoflush as sess:
    paths = [x[0] for x in sess.query(Version.fullpath).filter(Version.filename == fname)]
    if not paths:
        logger.info("Found no instances of {0}".format(fname))
        sys.exit(0)

    logger.info("Found the following instances")
    for path in paths:
        logger.info(" - {0}".format(path))

    p_and_f = []
    for p in paths:
        try:
            print ("Opening {0}".format(p))
            fits = pf.open(bz2.BZ2File(p))
            fits.verify(verifyopt)
            p_and_f.append((p, fits))
        except (pf.verify.VerifyError, IOError) as e:
            print (e)
    for ((afp, aobj), (bfp, bobj)) in combinations(p_and_f, 2):
        logger.info("-------------------------------------------------------------------------")
        logger.info("Differences between:")
        logger.info("  - {0}".format(afp))
        logger.info("  - {0}".format(bfp))

        for (ha, hb) in zip(aobj, bobj):
            print (pf.HeaderDiff(ha.header, hb.header).report())
