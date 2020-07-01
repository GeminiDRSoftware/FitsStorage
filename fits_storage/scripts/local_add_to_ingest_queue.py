#!/usr/bin/env python
import os
import re
import datetime
import time

from fits_storage.orm import session_scope
from fits_storage.fits_storage_config import storage_root
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.utils.ingestqueue import IngestQueueUtil


if __name__ == "__main__":
    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--file-re", action="store", type="string", dest="file_re", help="python regular expression string to select files by. Special values are today, twoday, fourday to include only files from today, the last two days, or the last four days respectively (days counted as UTC days)")
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")
    parser.add_option("--path", action="store", dest="path", default = "", help="Use given path relative to storage root")

    (options, args) = parser.parse_args()
    path = options.path

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Annouce startup
    logger.info("*********    local_add_to_ingest_queue.py - starting up at %s" % datetime.datetime.now())

    # Get a list of all the files in the datastore

    fulldirpath = os.path.join(storage_root, path)
    logger.info("Queueing files for ingest from: %s" % fulldirpath)

    filelist = []
    for root, dirs, files in os.walk(fulldirpath):
        if ".svn" in root:
            continue

        print("Ingesting:", root)
        filelist.extend([os.path.abspath(os.path.join(root, fn)) for fn in files])

    files = filelist

    # Skip files with tmp in the filename
    # Also require .fits in the filename
    thefiles = []
    tmpcre = re.compile("tmp")
    fitscre = re.compile(".fits$")
    logger.info("Checking for tmp files")
    for filename in files:
        if(tmpcre.search(filename) or not fitscre.search(filename)):
            logger.info("skipping tmp file: %s" % filename)
        else:
            thefiles.append(filename)

    i = 0
    n = len(thefiles)
    # print what we're about to do, and give abort opportunity
    logger.info("About to scan %d files" % n)
    if (n > 5000):
        logger.info("That's a lot of files. Hit ctrl-c within 5 secs to abort")
        time.sleep(6)

    with session_scope() as session:
        iq = IngestQueueUtil(session, logger)

        for fullfilename in thefiles:
            filename = os.path.basename(fullfilename)
            path = os.path.dirname(fullfilename)
            if storage_root in path:
                path = path[len(storage_root)+1:]
            i += 1
            logger.info("Queueing for Ingest: (%d/%d): %s" % (i, n, filename))

            iq.add_to_queue(filename, path)

    logger.info("*** local_add_to_ingestqueue.py exiting normally at %s" % datetime.datetime.now())
