#!/usr/bin/env python3

import datetime
import os
import smtplib
import time
from optparse import OptionParser

from fits_storage.logger import logger, setdebug, setdemon

from fits_storage.server.tapeutils import FileOnTapeHelper
from fits_storage.core.hashes import md5sum

from fits_storage.config import get_config
fsc = get_config()

parser = OptionParser()
parser.add_option("--tapeserver", action="store", type="string",
                  dest="tapeserver", default=fsc.tape_server,
                  help="FitsStorage Tape server to check the files are on tape")
parser.add_option("--dir", action="store", type="string", dest="dir",
                  default="", help="Directory to delete files from")
parser.add_option("--file-pre", action="store", type="string", dest="filepre",
                  help="File prefix to operate on, eg N20090130, N200812 etc")
parser.add_option("--yesimsure", action="store_true", dest="yesimsure",
                  help="Needed when file count is large")
parser.add_option("--minage", type="int", action="store", dest="minage",
                  help="Minimum days old a file must be to be deleted")
parser.add_option("--mintapes", action="store", type="int", dest="mintapes",
                  default=2, help="Minimum number of tapes file must be on to "
                                  "be eligible for deletion")
parser.add_option("--skip-md5", action="store_true", dest="skipmd5",
                  help="Do not bother to verify the md5 of the file on disk")
parser.add_option("--dryrun", action="store_true", dest="dryrun",
                  help="Dry Run - do not actually do anything")
parser.add_option("--emailto", action="store", type="string", dest="emailto",
                  help="Email address to send message to")
parser.add_option("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon",
                  help="Run as a background demon, do not generate stdout")
(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Announce startup
logger.info("***   local_delete_files.py - starting up at %s",
            datetime.datetime.now())

if not options.dir:
    logger.error("You must specify a directory with --dir")
    exit(1)
    
# Get a list of files in the directory
dirfiles = os.scandir(options.dir)

# Down select that list of files based on filepre and minage
candidates = []
for df in dirfiles:
    if df.is_file and df.name.startswith(options.filepre):
        if options.minage:
            minage = options.minage * 86400  # Days to seconds
            secs_now = time.time()  # Gives now() in seconds from unix epoch
            minage_secs = secs_now - minage  # Unix seconds at minage threshold
            if df.stat().m_time > minage_secs:  # File was modified "recently"
                candidates.append(df.name)
                logger.debug('File %s met filepre and minage criteria', df.name)
            else:
                logger.debug('File %s did not meet minage criteria', df.name)
        else:
            # File met filepre criteria and minage was not specified
            logger.debug('File %s met filepre criteria', df.name)
            candidates.append(df.name)
    else:
        # File did not meet filepre criteria
        logger.debug('File %s did not match filepre', df.name)

if len(candidates) == 0:
    logger.info("No files found matching criteria, exiting")
    exit(0)
logger.info("Found %d files to consider for deletion", len(candidates))

if len(candidates) > 2000 and not options.yesimsure:
    logger.error("To proceed with this many files, you must say --yesimsure")
    exit(1)

# We use the FileOnTapeHelper class here which provides caching..
foth = FileOnTapeHelper(tapeserver=options.tapeserver, logger=logger)

if options.filepre:
    logger.info("Pre-populating tape server results cache from filepre")
    foth.populate_cache(options.filepre)

numfiles = 0
for candidate in candidates:
    logger.debug('Considering: %d', candidate)
    fullpath = os.path.join(options.dir, candidate)

    if options.skipmd5:
        logger.debug("Skipping md5 check")
        filemd5 = None
    else:
        filemd5 = md5sum(fullpath)

    # foth.check_file() skips the md5 check if passed None for the md5 value
    tape_ids = foth.check_file(candidate, filemd5)

    if len(tape_ids) < options.mintapes:
        logger.info("File %s is only on %d tapes (%s), not deleting",
                    candidate, len(tape_ids), str(tape_ids))
        continue

    # If we got here, the candidate meets the criteria to delete
    if options.dryrun:
        logger.info("Dryrun: not actually deleting file %s", candidate)
    else:
        try:
            logger.info("Deleting file %s", fullpath)
            os.unlink(fullpath)
            numfiles += 1

        except Exception:
            logger.error("Could not delete %s", fullpath, exc_info=True)

logger.info("Deleted %d files.", numfiles)
    
if options.emailto:
    if options.dryrun:
        subject = "Dry run local file delete report"
    else:
        subject = "Local file delete report"

    mailfrom = 'fitsdata@gemini.edu'
    mailto = [options.emailto]

    msglines = [
        "Deleted %d files." % numfiles,
    ]

    message = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" % \
              (mailfrom, ", ".join(mailto), subject, '\n'.join(msglines))

    server = smtplib.SMTP(fsc.smtp_server)
    server.sendmail(mailfrom, mailto, message)
    server.quit()

logger.info("***   local_delete_files.py exiting normally at %s",
            datetime.datetime.now())
