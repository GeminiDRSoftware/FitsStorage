import os
import re
import datetime
import subprocess

from fits_storage.fits_storage_config import fits_db_backup_dir, fits_dbname
from fits_storage.logger import logger, setdebug, setdemon

datestring = datetime.datetime.now().isoformat()

from optparse import OptionParser
parser = OptionParser()
parser.add_option("--dontdelete", action="store_true", dest="dontdelete", help="Don't actually delete anything, just say what would be deleted")
parser.add_option("--dontbackup", action="store_true", dest="dontbackup", help="Don't back up the database, just clean up")
parser.add_option("--exclude-queues", action="store_true", dest="queues", help="Dont dump the queue tables as this conflicts with their locking and causes the queues to hang up during backup.")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
now = datetime.datetime.now()
logger.info("*********    database_backup.py - starting up at %s" % now)


# BACKUP STUFF
if not options.dontbackup:
    # The backup filename
    filename = "%s.%s.pg_dump_c" % (fits_dbname, datestring)
    if options.queues:
        command = ["/usr/bin/pg_dump", "--format=c", "--file=%s/%s" % (fits_db_backup_dir, filename), '--exclude-table=*queue', fits_dbname]
    else:
        command = ["/usr/bin/pg_dump", "--format=c", "--file=%s/%s" % (fits_db_backup_dir, filename), fits_dbname]

    logger.info("Executing pg_dump")
    sp = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdoutstring, stderrstring) = sp.communicate()
    logger.info(stderrstring)
    logger.info(stdoutstring)

    logger.info("-- Finished, Exiting")


# CLEANUP STUFF
split_date = now.isoformat().split('T')[0].split('-')

# Strip date
year = int(split_date[0])
month = int(split_date[1])
day = int(split_date[2])
today = datetime.date(year, month, day)

db_backup = os.listdir(fits_db_backup_dir)

for filename in db_backup:
    # Strip filename
    match = re.match('fitsdata.(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}).(\d{6}).pg_dump_c', filename)
    try:
        datematch = match.groups()[:3]
        y, m, d = int(datematch[0]), int(datematch[1]), int(datematch[2])
        date = datetime.date(y, m, d)

        if date > today:
            logger.error("The file %s has a date larger than today's date" % filename)
            break

        time_difference = (today-date).days
        # Filter though files from the same year
        if time_difference <= 365:
            # Keep one file per day for the past 10 days
            if time_difference < 10:
                logger.info("This file is less than 10 days old: %s" % filename)
            # Keep one file every 10 days in the last 2 months
            elif time_difference < 60 and d in (11, 21):
                logger.info("This file is less than 2 months old and falls on 01, 11 or 21 of the month: %s" % filename)
            # Keep one file per month for the past year
            elif d == 1:
                logger.info("This file is less than 1 year old and falls on 01 of the month: %s" % filename)
            else:
                if options.dontdelete:
                    logger.info("This file would be deleted: %s" % filename)
                else:
                    os.remove("%s/%s" % (fits_db_backup_dir, filename))
                    logger.info("Deleting file: %s" % filename)
        else:
            if options.dontdelete:
                logger.info("This file would be deleted: %s" % filename)
            else:
                logger.info("Deleting file: %s" % filename)
                os.remove("%s/%s" % (fits_db_backup_dir, filename))
    except AttributeError:
        # No match
        logger.info("The file %s is not in the expected format." % filename)

