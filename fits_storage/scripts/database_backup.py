#! /usr/bin/env python3

import os
import re
import datetime
import subprocess

from fits_storage.config import get_config
from fits_storage.logger import logger, setdebug, setdemon


from argparse import ArgumentParser
parser = ArgumentParser(prog='database_backup.py',
                        description="Backup the postgres database")
parser.add_argument("--dontdelete", action="store_true", dest="dontdelete",
                    help="Don't actually delete any old backups, just say "
                         "what would be deleted")
parser.add_argument("--dontbackup", action="store_true", dest="dontbackup",
                    help="Don't back up the database, just clean up")
parser.add_argument("--exclude-queues", action="store_true", dest="queues",
                    help="Dont dump the queue tables as this conflicts with "
                         "their locking and causes the queues to hang up "
                         "during backup.")
parser.add_argument("--debug", action="store_true", dest="debug",
                    help="Increase log level to debug")
parser.add_argument("--demon", action="store_true", dest="demon",
                    help="Run as a background demon, do not generate stdout")

options = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# FitsStorage Configuration
fsc = get_config()

# Get a single 'now' value to use for consistency
now = datetime.datetime.now()

# Announce startup
logger.info("***   database_backup.py - starting up at %s", now)

# Check if destination directory exists
if not os.path.isdir(fsc.fits_db_backup_dir):
    logger.error("Backup Directory %s does not exist", fsc.fits_db_backup_dir)
    exit(1)
if not os.access(fsc.fits_db_backup_dir, os.W_OK):
    logger.error("No write permission to Backup Directory %s",
                 fsc.fits_db_backup_dir)
    exit(1)

# Do the pg_dump backup
if not options.dontbackup:

    filename = f"{fsc.fits_db_backup_dir}/fitsdata." \
               f"{datetime.datetime.now().isoformat()}.pg_dump_c"
    logger.debug("Backup Filename is %s", filename)

    command = ["/usr/bin/pg_dump",
               "--format=c",
               f"--file={filename}"]
    if options.queues:
        command.append('--exclude-table=*queue*')
    command.append(fsc.fits_dbname)
    logger.debug("Command is: %s", str(command))

    logger.info("Executing pg_dump...")
    sp = subprocess.Popen(command, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)
    (stdoutstring, stderrstring) = sp.communicate()
    logger.info(stderrstring)
    logger.info(stdoutstring)
    logger.info("... pg_dump complete.")

# Cleanup old backup files
today = now.date()

backup_files = os.listdir(fsc.fits_db_backup_dir)

for filename in backup_files:
    # Strip filename
    match = re.match(r'fitsdata.(\d{4})-(\d{2})-(\d{2})'
                     r'T(\d{2}):(\d{2}):(\d{2}).(\d{6}).pg_dump_c', filename)
    try:
        datematch = match.groups()[:3]
        y, m, d = int(datematch[0]), int(datematch[1]), int(datematch[2])
        date = datetime.date(y, m, d)

        if date > today:
            logger.error("The file %s has a date larger than today's date",
                         filename)
            break

        time_difference = (today-date).days
        # Filter though files from the same year
        if time_difference <= 365:
            # Keep one file per day for the past 10 days
            if time_difference < 10:
                logger.info("This file is less than 10 days old: %s",
                            filename)
            # Keep one file every 10 days in the last 2 months
            elif time_difference < 60 and d in (11, 21):
                logger.info("This file is less than 2 months old and falls "
                            "on 01, 11 or 21 of the month: %s", filename)
            # Keep one file per month for the past year
            elif d == 1:
                logger.info("This file is less than 1 year old and falls on"
                            " 01 of the month: %s", filename)
            else:
                if options.dontdelete:
                    logger.info("This file would be deleted: %s", filename)
                else:
                    os.remove("%s/%s" % (fsc.fits_db_backup_dir, filename))
                    logger.info("Deleting file: %s", filename)
        else:
            if options.dontdelete:
                logger.info("This file would be deleted: %s", filename)
            else:
                logger.info("Deleting file: %s", filename)
                os.remove("%s/%s" % (fsc.fits_db_backup_dir, filename))
    except AttributeError:
        # No match
        logger.info("The file %s is not in the expected format.", filename)
