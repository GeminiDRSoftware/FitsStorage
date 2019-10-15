#! /usr/bin/env python

import os
import re
import datetime
import time

from fits_storage.orm import session_scope
from fits_storage.fits_storage_config import storage_root, using_s3
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.utils.ingestqueue import IngestQueueUtil

if using_s3:
    from fits_storage.utils.aws_s3 import get_helper
    s3 = get_helper()

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

parser.add_argument("--path", action="store", dest="path", default = "",
                    help="Use given path relative to storage root")

parser.add_argument("--force", action="store_true", dest="force", default=False,
                    help="Force re-ingestion of these files unconditionally")

parser.add_argument("--force_md5", action="store_true", dest="force_md5",
                    default=False, help="Force md5 file check, not just lastmod date")

parser.add_argument("--after", action="store", dest="after", default = None,
                    help="Ingest only after this datetime")

parser.add_argument("--newfiles", action="store", type=int, dest="newfiles",
                    default=None, help="Only queue files modified in the last N days")

parser.add_argument("--filename", action="store", type=str, dest="filename",
                    default=None, help="Just add this one filename to the queue")

parser.add_argument("--listfile", action="store", type=str, dest="listfile",
                    default=None, help="Read filenames to add from this text file")

options = parser.parse_args()
path = options.path

# ------------------------------------------------------------------------------
# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
now = datetime.datetime.now()
logger.info("*********    add_to_ingest_queue.py - starting up at {}".format(now))

# Get a list of all the files in the datastore
# We assume this is just one dir (ie non recursive) for now.
gotfiles = False

if options.filename:
    logger.info("Adding single file: {}".format(options.filename))
    files = [options.filename]
    gotfiles = True

if options.listfile:
    logger.info("Adding files from list file: {}".format(options.listfile))
    with open(options.listfile) as f:
        filesread = f.readlines()
    files = []
    for l in filesread:
        files.append(l.strip())
    gotfiles = True

if gotfiles is False:
    if using_s3:
        logger.info("Querying files for ingest from S3 bucket")
        filelist = s3.key_names()
    else:
        fulldirpath = os.path.join(storage_root, path)
        logger.info("Queueing files for ingest from: {}".format(fulldirpath))
        filelist = os.listdir(fulldirpath)

    logger.info("Got file list.")

    file_re = options.file_re
    # Handle the today and twoday etc options
    now = datetime.datetime.utcnow()
    delta = datetime.timedelta(days=1)
    named_intervals = {
        'today': 1,
        'twoday': 2,
        'fourday': 3,
        'tenday': 10,
        'twentyday': 20
        }

    if options.file_re in named_intervals:
        then = now.date()
        dates = [then-(delta*n) for n in range(named_intervals[options.file_re])]
        file_re = '|'.join(d.strftime("%Y%m%d") for d in dates)

    files = []
    if file_re:
        cre = re.compile(file_re)
        files = list(filter(cre.search, filelist))
    else:
        files = filelist

# Skip various tmp files
# Also require .fits in the filename
thefiles = []
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
    iq = IngestQueueUtil(session, logger)
    for i, filename in enumerate(thefiles, 1):
        if options.newfiles:
            fullpath = os.path.join(fulldirpath, filename)
            mtime = datetime.datetime.fromtimestamp(os.path.getmtime(fullpath))
            age = now - mtime
            age = age.total_seconds()
            if age > newfiles_seconds:
                logger.debug("Skipping {}: older than {}s".format(filename, newfiles_seconds))
                continue

        logger.info("Queueing for Ingest: ({}/{}): {}".format(i, n, filename))
        iq.add_to_queue(filename, path, force=options.force,
                        force_md5=options.force_md5, after=options.after)

logger.info("*** add_to_ingestqueue.py exiting normally at {}".format(datetime.datetime.now()))
