import datetime
import sys

from sqlalchemy import join, desc

from orm import sessionfactory
from orm.diskfile import DiskFile
from orm.file import File
from fits_storage_config import using_s3, aws_access_key, aws_secret_key, s3_bucket_name
from logger import logger, setdebug, setdemon
from boto.s3.connection import S3Connection


# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--file-pre", action="store", type="string", dest="filepre", help="File prefix to operate on, eg N20090130, N200812 etc")
parser.add_option("--yesimsure", action="store_true", dest="yesimsure", default=False, help="Needed for sanity check")
parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Dry Run - do not actually do anything")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)


# Annouce startup
logger.info("*********    s3_unconditional_delete_files.py - starting up at %s" % datetime.datetime.now())

if(using_s3 == False):
    logger.error("This script is only useable on installations using S3 for storage")
    sys.exit(1)

if(options.yesimsure != True):
    logger.info("This is a really dangerous script to run. If you're not sure, don't do this.")
    logger.info("This will unconditionally delete files from the S3 storage")
    logger.error("You need to say --yesimsure to make it work")
    sys.exit(2)

if(not options.filepre or len(options.filepre) < 5):
    logger.error("filepre is dangerously short, please re-think what youre doing")
    sys.exit(3)

session = sessionfactory()

query = session.query(DiskFile.id).select_from(join(File, DiskFile)).filter(DiskFile.present==True)
likestr = "%s%%" % options.filepre
query = query.filter(File.name.like(likestr))

diskfileids = query.all()

if(len(diskfileids) == 0):
    logger.info("No Files found matching file-pre. Exiting")
    session.close()
    sys.exit(0)

logger.info("Got %d files for deletion" % len(diskfileids))

s3conn = S3Connection(aws_access_key, aws_secret_key)
bucket = s3conn.get_bucket(s3_bucket_name)


for diskfileid in diskfileids:
    diskfile = session.query(DiskFile).filter(DiskFile.id == diskfileid).one()
    logger.info("Deleting file %s" % diskfile.filename)
    key = bucket.get_key(diskfile.filename)
    if(key is None):
        logger.error("File %s did not exist on S3 anyway!" % diskfile.filename)   
        diskfile.present = False
    else:
        key.delete()
        diskfile.present = False
    session.commit()
    
session.close()
logger.info("** s3_unconditional_delete_files.py exiting normally")
