#!/usr/bin/env python3

import datetime
from sqlalchemy import desc
from optparse import OptionParser

from fits_storage.db import sessionfactory
from fits_storage.server.orm.tapestuff import Tape, TapeWrite, TapeFile, \
    TapeRead
from fits_storage.logger import logger, setdebug, setdemon


# Option Parsing
parser = OptionParser()
parser.add_option("--file-re", action="store", type="string", dest="filere",
                  help="Regular expression used to select files")
parser.add_option("--tape-label", action="store", type="string",
                  dest="tape_label",
                  help="Request only files that are on this tape.")
parser.add_option("--from-set", action="store", type="int", dest="from_set",
                  help="The tape set number to get files from")
parser.add_option("--to-set", action="store", type="int", dest="to_set",
                  help="The tape set number to put files to")
parser.add_option("--to-set-tapes", action="store", type="int",
                  dest="to_set_tapes", default=1,
                  help="The number of distinct tapes in the to set that the "
                       "file should be on")
parser.add_option("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon",
                  help="Run as a background demon, do not generate stdout")
(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Announce startup
logger.info("***   tape_set_diff.py - starting up at %s",
            datetime.datetime.now())

import sys
logger.error("This script hasn't been properly tested since the great refactor"
             "of 2023. Please test before use")
sys.exit(1)

# Query the DB to find a list of files on the from set This is a little non-
# trivial, given that there are multiple identical copies of the file on
# several tapes and also that there can be multiple non-identical version of
# the file on tapes too. Also, we only care about the most recent (highest
# lastmod) version of any given filename

# OK. In the first pass, we simply get a list of filenames (that exist on
# valid tapewrites on valid tapes in the set) that we can extract. Then we do
# another pass to figure out which version of each file we are going to extract.
session = sessionfactory()

logger.debug("Making initial list of filenames query")
query = session.query(TapeFile.filename)\
    .select_from(Tape, TapeWrite, TapeFile)\
    .filter(Tape.id == TapeWrite.tape_id)\
    .filter(TapeWrite.id == TapeFile.tapewrite_id)\
    .filter(Tape.active == True).filter(TapeWrite.suceeded == True)

# Match against the given filere
if options.filere:
    query = query.filter(TapeFile.filename.contains(options.filere))

# Match against the given tape label
if options.tape_label:
    query = query.filter(Tape.label == options.tape_label)

# And of course the set number
query = query.filter(Tape.set == options.from_set)

query = query.distinct()

filenames = query.all()

# Now we loop through those filenames, finding the md5 for the version with
# the most recent lastmod
logger.debug("Looping through filenames, finding md5 of correct file instance")

for filename in filenames:
    filename = filename[0]
    logger.debug("Considering filename: %s", filename)
    query = session.query(TapeFile.md5).select_from(Tape, TapeWrite, TapeFile)\
        .filter(Tape.id == TapeWrite.tape_id)\
        .filter(TapeWrite.id == TapeFile.tapewrite_id)\
        .filter(TapeWrite.suceeded == True).filter(Tape.active == True)\
        .filter(Tape.set == options.from_set)\
        .filter(TapeFile.filename == filename)\
        .order_by(desc(TapeFile.lastmod))

    md5 = query.first()[0]

    # Now we have filename, md5 of all a "canonical" file in the from set

    # check how many tapes this is on in the destination set
    newquery = session.query(Tape).select_from(Tape, TapeWrite, TapeFile)\
        .filter(Tape.id == TapeWrite.tape_id)\
        .filter(TapeWrite.id == TapeFile.tapewrite_id)\
        .filter(TapeWrite.suceeded == True).filter(Tape.active == True)\
        .filter(Tape.set == options.to_set)\
        .filter(TapeFile.filename == filename).filter(TapeFile.md5 == md5)\
        .distinct()

    newtapes = newquery.all()

    if len(newtapes) >= options.to_set_tapes:
        logger.debug("Filename: %s is on sufficient tapes in destination tape "
                     "set already", filename)
    else:
        logger.info("Adding %s - %s to request list", (filename, md5))

        # Add the filename, md5 to the taperead table
        tr = TapeRead()
        tr.filename = filename
        tr.md5 = md5

        session.add(tr)
        session.commit()

    session.close()
