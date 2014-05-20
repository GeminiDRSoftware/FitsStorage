from orm import sessionfactory
from fits_storage_config import storage_root, using_s3
from logger import logger, setdebug, setdemon
from utils.ingestqueue import add_to_ingestqueue
import os
import re
import datetime
import time
if (using_s3):
    from fits_storage_config import s3_bucket_name, aws_access_key, aws_secret_key
    from boto.s3.connection import S3Connection

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

(options, args) = parser.parse_args()
path = options.path

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
now = datetime.datetime.now()
logger.info("*********    add_to_ingest_queue.py - starting up at %s" % now)

# Get a list of all the files in the datastore
# We assume this is just one dir (ie non recursive) for now.

if(using_s3):
    logger.info("Querying files for ingest from S3 bucket: %s" % s3_bucket_name)
    s3conn = S3Connection(aws_access_key, aws_secret_key)
    bucket = s3conn.get_bucket(s3_bucket_name)
    filelist = []
    for key in bucket.list():
        filelist.append(key.name)
else:
    fulldirpath = os.path.join(storage_root, path)
    logger.info("Queueing files for ingest from: %s" % fulldirpath)
    filelist = os.listdir(fulldirpath)

logger.info("Got file list.")

file_re = options.file_re
# Handle the today and twoday etc options
now = datetime.datetime.utcnow()
delta = datetime.timedelta(days=1)
if(options.file_re == "today"):
    file_re = now.date().strftime("%Y%m%d")

if(options.file_re == "twoday"):
    then = now-delta
    a = now.date().strftime("%Y%m%d")
    b = then.date().strftime("%Y%m%d")
    file_re = "%s|%s" % (a, b)

if(options.file_re == "fourday"):
    a = now.date().strftime("%Y%m%d")
    then = now - delta
    b = then.date().strftime("%Y%m%d")
    then = then - delta
    c = then.date().strftime("%Y%m%d")
    then = then - delta
    d = then.date().strftime("%Y%m%d")
    file_re = "%s|%s|%s|%s" % (a, b, c, d)

if(options.file_re == "tenday"):
    list = []
    then = now
    for i in range(10):
        list.append(then.date().strftime("%Y%m%d"))
        then = then-delta
    file_re = '|'.join(list)

if(options.file_re == "twentyday"):
    list = []
    then = now
    for i in range(20):
        list.append(then.date().strftime("%Y%m%d"))
        then = then-delta
    file_re = '|'.join(list)

if(file_re):
    cre = re.compile(file_re)


files = []
if(file_re):
    for filename in filelist:
        if(cre.search(filename)):
            files.append(filename)
else:
    files = filelist

# Skip various tmp files
# Also require .fits in the filename
thefiles = []
tmpcre = re.compile("(tmp)|(tiled)")
fitscre = re.compile(".fits")
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
if (n>5000):
    logger.info("That's a lot of files. Hit ctrl-c within 5 secs to abort")
    time.sleep(6)

session = sessionfactory()

for filename in thefiles:
    i += 1
    logger.info("Queueing for Ingest: (%d/%d): %s" % (i, n, filename))
    add_to_ingestqueue(session, filename, path, force=options.force, force_md5=options.force_md5, after=options.after)

session.close()
logger.info("*** add_to_ingestqueue.py exiting normally at %s" % datetime.datetime.now())

