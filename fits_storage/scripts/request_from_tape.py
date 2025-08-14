#!/usr/bin/env python3

import sys
import datetime
import requests
from sqlalchemy import desc
from optparse import OptionParser

from fits_storage.db import sessionfactory
from fits_storage.server.orm.tapestuff import Tape, TapeWrite, TapeFile, \
    TapeRead
from fits_storage.logger import logger, setdebug, setdemon

# Option Parsing
parser = OptionParser()
parser.add_option("--file-re", action="store", type="string", dest="filere",
                  help="Regular expression used to select files to extract")
parser.add_option("--tape-label", action="store", type="string",
                  dest="tape_label",
                  help="Request only files that are on this tape.")
parser.add_option("--no-really", action="store_true", dest="noreally",
                  help="over-ride the usual requirement to supply a --file-re "
                       "or --tape-label. Only useful when you really do want "
                       "to request all files in the database. "
                       "May the force be with you.")
parser.add_option("--all", action="store_true", dest="all",
                  help="When multiple versions of a file are on tape, "
                       "get them all, not just the most recent")
parser.add_option("--jsonfilelist", action="store", dest="jsonfilelist",
                  help="Read list to extract from this jsonfilelist URL")
parser.add_option("--dryrun", action="store_true", dest="dryrun",
                  help="Dry Run - do not actually do anything")
parser.add_option("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon",
                  help="Run as a background demon, do not generate stdout")
(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Announce startup
logger.info("***   request_from_tape.py - starting up at %s",
            datetime.datetime.now())

if ((not options.filere) and (not options.tape_label) and
        (not options.noreally) and (not options.jsonfilelist)):
    logger.error("You must specify a file-re or a tape-label. "
                 "You probably want a file-re")
    sys.exit(1)

session = sessionfactory()

if options.jsonfilelist:
    # This bypasses the usual file version logic, specifically requesting the
    # version with the md5 from the filelist
    r = requests.get(options.jsonfilelist)
    if r.status_code != 200:
        logger.error(f"Bad status code {r.status_code} reading "
                     f"{options.jsonfilelist}. Exiting")
        exit(1)
    jfl = r.json()

    for jf in jfl:
        tr = TapeRead()
        tr.filename = jf['filename']
        tr.md5 = jf['file_md5']
        logger.info(f"Adding {tr.filename}, {tr.md5} to taperead")
        session.add(tr)
    session.commit()


else:
    # Query the DB to find a list of files to extract This is a little
    # non-trivial, given that there are multiple identical copies of the file
    # on several tapes and also that there can be multiple non-identical
    # version of the file on tapes too. OK. In the first pass, we simply get
    # a list of filenames (that exist on valid tapewrites on valid tapes)
    # that we can extract. Then we do another pass to figure out which
    # version of each file we are going to extract.

    query = session.query(TapeFile.filename)\
        .select_from(Tape, TapeWrite, TapeFile)\
        .filter(Tape.id == TapeWrite.tape_id)\
        .filter(TapeWrite.id == TapeFile.tapewrite_id)

    # Match against the given filere
    if options.filere:
        query = query.filter(TapeFile.filename.contains(options.filere))

    if options.tape_label:
        query = query.filter(Tape.label == options.tape_label)
    else:
        # Other housekeeping checks - tape should be active, unless a tape-label
        # request as you might be trying to recover from a bad tape
        query = query.filter(Tape.active == True)

        # Other housekeeping checks - if the write never succeeded, we probably
        # don't care about it
        query = query.filter(TapeWrite.succeeded == True)

    query = query.distinct()

    filenames = query.all()

    # Now we loop through those filenames, finding the md5 for the version with
    # the most recent lastmod

    for filename in filenames:
        query = session.query(TapeFile.md5)\
            .select_from(Tape, TapeWrite, TapeFile)\
            .filter(Tape.id == TapeWrite.tape_id)\
            .filter(TapeWrite.id == TapeFile.tapewrite_id)\
            .filter(TapeWrite.succeeded == True).filter(Tape.active == True)\
            .filter(TapeFile.filename == filename[0])\
            .order_by(desc(TapeFile.lastmod))

        md5 = query.first()

        # Add the filename, md5 to the taperead table
        tr = TapeRead()
        tr.filename = filename[0]
        tr.md5 = md5[0]
        logger.info("Adding %s, %s to taperead", filename[0], md5[0])

        session.add(tr)
        session.commit()

session.close()

logger.info("***   request_from_tape.py - exiting at %s",
            datetime.datetime.now())
