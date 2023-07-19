#! /usr/bin/env python3

import os
import re
import datetime
import time

from fits_storage.config import get_config
fsc = get_config()

from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix

from fits_storage.db import session_scope
from fits_storage.queues.queue import IngestQueue

if fsc.using_s3:
    from fits_storage.server.aws_s3 import get_helper
    s3 = get_helper()


"""
Script to add files to the ingest queue.
"""


def _dayoptions(string):
    # Function to parse the today and twoday etc. options
    # Returns a compiled regex that can be used with search
    oneday = datetime.timedelta(days=1)
    named_intervals = {
        'today': 1,
        'twoday': 2,
        'fourday': 4,
        'tenday': 10,
        'twentyday': 20
    }

    if string in named_intervals:
        today = datetime.datetime.utcnow().date()
        dates = [today - (oneday * n) for n in range(named_intervals[string])]
        regex = '|'.join(d.strftime("%Y%m%d") for d in dates)
        return re.compile(regex)
    else:
        return re.compile(string)


if __name__ == "__main__":
    # Option Parsing
    from argparse import ArgumentParser
    # ------------------------------------------------------------------------------
    parser = ArgumentParser()
    parser.add_argument("--file-re", action="store", type=str, dest="file_re",
        help="python regular expression string to select files. "
        "Special values are today, twoday, fourday, tenday twentyday "
        "to include only files from today, the last two days, the last "
        "four days, or the last 10 days respectively (days counted as UTC days)")

    parser.add_argument("--debug", action="store_true", dest="debug",
                        help="Increase log level to debug")

    parser.add_argument("--demon", action="store_true", dest="demon",
                        help="Run as a background demon, do not generate stdout")

    parser.add_argument("--path", action="store", dest="path", default="",
                        help="Use given path relative to storage root")

    parser.add_argument("--force", action="store_true", dest="force", default=False,
                        help="Force re-ingestion of these files unconditionally")

    parser.add_argument("--force_md5", action="store_true", dest="force_md5",
                        default=False, help="Force md5 file check, not just lastmod date")

    parser.add_argument("--after", action="store", dest="after", default=None,
                        help="Ingest only after this UTC datetime")

    parser.add_argument("--newfiles", action="store", type=int, dest="newfiles",
                        default=None, help="Only queue files modified in the last N days")

    parser.add_argument("--filename", action="store", type=str, dest="filename",
                        default=None, help="Just add this one filename to the queue")

    parser.add_argument("--listfile", action="store", type=str, dest="listfile",
                        default=None, help="Read filenames to add from this text file")

    parser.add_argument("--logsuffix", action="store", type=str, dest="logsuffix",
                        default=None, help="Extra suffix to add on logfile")

    options = parser.parse_args()
    path = options.path

    # ------------------------------------------------------------------------------
    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Check Log Suffix
    if options.logsuffix:
        setlogfilesuffix(options.logsuffix)

    # Announce startup
    logger.info("*** add_to_ingest_queue.py - starting up at {}"
                .format(datetime.datetime.now()))
    logger.debug("Config files used: %s", ', '.join(fsc.configfiles_used))

    if options.filename:
        # Just add a single filename
        logger.info("Adding single file: {}".format(options.filename))
        files = [options.filename]

    elif options.listfile:
        # Get list of files from list file
        logger.info("Adding files from list file: {}".format(options.listfile))
        files = []
        with open(options.listfile) as f:
            for line in f:
                files.append(line.strip())

    else:
        # Read directory to get file list.
        if fsc.using_s3:
            logger.info("Querying files for ingest from S3 bucket")
            fulllist = s3.key_names()
        else:
            fulldirpath = os.path.join(fsc.storage_root, path)
            logger.info("Queueing files for ingest from: {}".format(fulldirpath))
            fulllist = os.listdir(fulldirpath)

        logger.info("Got full file list.")

        # Handle the file_re regex, including 'today', 'twoday', etc
        if options.file_re:
            cre = _dayoptions(options.file_re)
            files = list(filter(cre.search, fulllist))
        else:
            files = fulllist

    # Skip various tmp files
    # Also require .fits in the filename
    tmpcre = re.compile("(tmp)|(tiled)")
    fitscre = re.compile(".fits")
    obslogcre = re.compile("_obslog.txt")
    previewcre = re.compile("_preview.jpg")
    miscfilecre = re.compile("miscfile_")
    logger.info("Checking for tmp files")

    def skip_file(filename):
        return (
            tmpcre.search(filename)
            or previewcre.search(filename)
            or not (fitscre.search(filename)
                    or obslogcre.search(filename)
                    or miscfilecre.search(filename))
         )

    thefiles = []
    for filename in files:
        if skip_file(filename):
            logger.info("skipping tmp file: {}".format(filename))
        else:
            thefiles.append(filename)

    n = len(thefiles)
    # print what we're about to do, and give abort opportunity
    logger.info("About to scan {} files".format(n))
    if n > 5000:
        logger.info("That's a lot of files. Hit ctrl-c within 5 secs to abort")
        time.sleep(6)

    if options.newfiles:
        now = datetime.datetime.now()
        newfiles_seconds = options.newfiles * 86400

    with session_scope() as session:
        iq = IngestQueue(session, logger=logger)
        for i, filename in enumerate(thefiles, 1):
            if options.newfiles:
                fullpath = os.path.join(fulldirpath, filename)
                mtime = datetime.datetime.fromtimestamp(os.path.getmtime(fullpath))
                age = now - mtime
                age = age.total_seconds()
                if age > newfiles_seconds:
                    logger.debug("Skipping {}: older than {}s".format(filename, newfiles_seconds))
                    continue
            if options.after:
                after = datetime.datetime.fromisoformat(options.after)
            else:
                after = None
            logger.info("Queueing for Ingest: ({}/{}): {}".format(i, n, filename))
            iq.add(filename, path, force=options.force,
                   force_md5=options.force_md5, after=after)

    logger.info("*** add_to_ingestqueue.py exiting normally at {}".format(datetime.datetime.now()))
