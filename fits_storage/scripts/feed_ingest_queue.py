from gemini_obs_db import session_scope
from fits_storage.fits_storage_config import storage_root, using_s3, fits_lockfile_dir
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.utils.ingestqueue import IngestQueueUtil
from fits_storage.utils.pidfile import PidFile, PidFileError
import sys
import os
import re
import datetime
import time

ONEDAY = datetime.timedelta(days=1)


# I think this is deprecated


def filebase(site, date):
    """
    Given a site ('N' or 'S') and date object,
    return the filename base
    eg N20140234
    """

    return "{}{}".format(site, date.strftime('%Y%m%d'))

def gemfilename(site, date, num):
    """
    Given a site ('N' or 'S'), a date object and a file number,
    return the filename string - eg N20140203S1234.fits
    """

    return "{}S{:04d}.fits".format(filebase(site, date), num)

def lookfordate(site, date, dirlist):
    """
    Given a site ('N' or 'S') and date object,
    and directory listing, look to see if there are
    files from that site and date in the list
    """

    found = False
    base = filebase(site, date)
    i = 0
    while i < len(dirlist) and not found:
        found = dirlist[i].startswith(base)
        i += 1

    return found


if __name__ == "__main__":
    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")
    parser.add_option("--path", action="store", dest="path", default="", help="Use given path relative to storage root")
    parser.add_option("--force", action="store_true", dest="force", default=False, help="Force re-ingestion of these files unconditionally")
    parser.add_option("--force_md5", action="store_true", dest="force_md5", default=False, help="Force checking of file change by md5 not just lastmod date")
    parser.add_option("--site", action="store", dest="site", help="Gemini Site - N or S")
    parser.add_option("--lockfile", action="store_true", dest="lockfile", help="Use a lockfile to limit instances")


    (options, args) = parser.parse_args()
    path = options.path

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Annouce startup
    logger.info("*********    feed_ingest_queue.py - starting up at %s", datetime.datetime.now())

    if using_s3:
        logger.warning("feed_ingest_queue not applicable when using S3. Exiting")
        sys.exit(0)

    if not options.site:
        logger.error("Must supply Gemini Site")
        sys.exit(0)

    site = options.site[0]
    if site not in ['N', 'S']:
        logger.error("Invalid Gemini Site")
        sys.exit(0)

    try:
        with PidFile(logger, "feed_ingest_queue", dummy=not options.lockfile) as pidfile:
            fulldirpath = os.path.join(storage_root, path)
            logger.info("Feeding files for ingest from: %s", fulldirpath)

            # Find "latest" file that already exists.

            # Initialize monitoring filename. Start from the start of UT today
            ut_date = datetime.datetime.utcnow().date()

            # Get an initial directory listing
            logger.debug("Getting directory listing")
            dirlist = os.listdir(fulldirpath)

            # Do files exist for the "current" ut_date?
            logger.debug("Looking for files starting with %s", filebase(site, ut_date))
            found = lookfordate(site, ut_date, dirlist)

            if not found:
                # Are we stuck in yesterday?
                ut_date -= ONEDAY
                logger.debug("Looking for files starting with %s", filebase(site, ut_date))
                found = lookfordate(site, ut_date, dirlist)

            # So now, whether we found files or not, ut_date is out best guess date of where next files will appear

            # If we did find files, zip forward to highest file number present. If not assume, 1
            num = 0
            cre = re.compile(r'^%sS(\d{4}).fits$' % filebase(site, ut_date))
            if found:
                for filename in dirlist:
                    m = cre.match(filename)
                    if m:
                        n = int(m.group(1))
                        if n > num:
                            num = n
                logger.info("Most recent file is: %s", gemfilename(site, ut_date, num))


            num += 1
            logger.info("Starting looking from: %s", gemfilename(site, ut_date, num))

            # Done with this
            dirlist = None

            # Enter main loop
            loop = True
            lastfound = datetime.datetime.now()
            while loop:
                logger.debug("looking from: %s", gemfilename(site, ut_date, num))
                found = False
                # Check for next files in number seqence
                for i in range(0, 10):
                    filename = gemfilename(site, ut_date, num+i)
                    fullpath = os.path.join(fulldirpath, filename)
                    if os.path.exists(fullpath):
                        logger.info("Found new file, Queueing for Ingest: %s", filename)
                        # Get a database session
                        logger.debug("Getting Session")
                        with session_scope() as session:
                            logger.debug("Adding file")
                            IngestQueueUtil(session, logger).add_to_queue(filename, path, force=options.force, force_md5=options.force_md5)
                        logger.debug("Committed")
                        logger.debug("Closed Session")
                        num += i + 1
                        found = True
                        break

                if not found:
                    # Check for files at start of next ut_date
                    tomorrow = ut_date + ONEDAY
                    for i in range(1, 10):
                        filename = gemfilename(site, tomorrow, i)
                        fullpath = os.path.join(fulldirpath, filename)
                        if os.path.exists(fullpath):
                            logger.info("Found new on new day - file: %s", filename)
                            ut_date = tomorrow
                            num = i
                            found = True
                            break

                if not found:
                    # Is it more than a day since we last found anything?
                    if datetime.datetime.now() - lastfound > ONEDAY:
                        logger.info("It has been more than a day since we found anything. Giving up")
                        loop = False
                    else:
                        # Wait 5 secs
                        logger.debug("Didn't find anything. Sleeping 4 seconds")
                        time.sleep(4)
                else:
                    lastfound = datetime.datetime.now()
    except PidFileError as e:
        logger.error(str(e))

    logger.info("*** feed_ingestqueue.py exiting normally at %s", datetime.datetime.now())
