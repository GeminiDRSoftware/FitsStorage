from fits_storage.orm import session_scope
from fits_storage.fits_storage_config import storage_root, using_s3
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.utils.ingestqueue import IngestQueueUtil
import os
import re
import datetime
import time
if using_s3:
    from fits_storage.utils.aws_s3 import get_helper()
    s3 = get_helper()

# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--file-re", action="store", type="string", dest="file_re", help="python regular expression string to select files by. Special values are today, twoday, fourday, tenday twentyday to include only files from today, the last two days, the last four days, or the last 10 days respectively (days counted as UTC days)")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")
parser.add_option("--path", action="store", dest="path", default = "", help="Use given path relative to storage root")
parser.add_option("--force", action="store_true", dest="force", default = False, help="Force re-ingestion of these files unconditionally")
parser.add_option("--force_md5", action="store_true", dest="force_md5", default = False, help="Force checking of file change by md5 not just lastmod date")
parser.add_option("--after", action="store", dest="after", default = None, help="ingest only after this datetime")

options, args = parser.parse_args()
path = options.path

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
now = datetime.datetime.now()
logger.info("*********    add_to_ingest_queue.py - starting up at %s" % now)

# Get a list of all the files in the datastore
# We assume this is just one dir (ie non recursive) for now.

if using_s3:
    logger.info("Querying files for ingest from S3 bucket")
    filelist = s3.key_names()
else:
    fulldirpath = os.path.join(storage_root, path)
    logger.info("Queueing files for ingest from: %s" % fulldirpath)
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
    dates = [then-(delta*n) for n in range(named_intervals[options.file])]
    file_re = '|'.join(d.date().strftime("%Y%m%d") for d in dates)

files = []
if file_re:
    cre = re.compile(file_re)
    files = filter(cre.search, filelist)
else:
    files = filelist

# Skip various tmp files
# Also require .fits in the filename
thefiles = []
tmpcre = re.compile("(tmp)|(tiled)")
fitscre = re.compile(".fits")
obslogcre = re.compile("_obslog.txt")
previewcre = re.compile("_preview.jpg")
logger.info("Checking for tmp files")

def skip_file(filename):
    return (
        tmpcre.search(filename)
     or previewcre.search(filename)
     or not (fitscre.search(filename) or obslogcre.search(filename))
     )

for filename in files:
    if skip_file(filename):
        logger.info("skipping tmp file: %s" % filename)
    else:
        thefiles.append(filename)

n = len(thefiles)
# print what we're about to do, and give abort opportunity
logger.info("About to scan %d files" % n)
if n > 5000:
    logger.info("That's a lot of files. Hit ctrl-c within 5 secs to abort")
    time.sleep(6)

with session_scope() as session:
    iq = IngestQueueUtil(session, logger)
    for i, filename in enumerate(thefiles, 1):
        logger.info("Queueing for Ingest: (%d/%d): %s" % (i, n, filename))
        iq.add_to_queue(filename, path, force=options.force, force_md5=options.force_md5, after=options.after)

logger.info("*** add_to_ingestqueue.py exiting normally at %s" % datetime.datetime.now())
