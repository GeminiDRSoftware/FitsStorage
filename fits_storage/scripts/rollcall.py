from gemini_obs_db import session_scope
from gemini_obs_db.file import File
from gemini_obs_db.diskfile import DiskFile

from fits_storage.logger import logger, setdebug, setdemon

from sqlalchemy import join
import datetime
from optparse import OptionParser

from fits_storage.fits_storage_config import using_s3

if using_s3:
    from fits_storage.utils.aws_s3 import get_helper


"""
Utility for validating `~DiskFile`s as being present.

This script checks a set of files to check if they are present and marks 
them not present if they are not available.  This can be useful in bootstrapping
a system where you mark all canonical `~DiskFile`s present and run this to
clear out the ones that are no longer on disk.
"""

if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("--limit", action="store", type="int", help="specify a limit on the number of files to examine. The list is sorted by lastmod time before the limit is applied")
    parser.add_option("--file-pre", action="store", type="string", dest="filepre", help="File prefix to check (omit for all)")
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

    (options, args) = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)


    # Annouce startup
    logger.info("*********    rollcall.py - starting up at %s" % datetime.datetime.now())

    # Get a database session
    with session_scope() as session:
        # Get a list of all diskfile_ids marked as present
        query = session.query(DiskFile.id).select_from(join(DiskFile, File)).filter(DiskFile.present == True).order_by(DiskFile.lastmod)

        if(options.filepre):
            likestr = "%s%%" % options.filepre
            query = query.filter(File.name.like(likestr))

        # Did we get a limit option?
        if(options.limit):
            query = query.limit(options.limit)

        logger.info("evaluating number of rows...")
        n = query.count()
        logger.info("%d files to check" % n)

        logger.info("Starting checking...")

        if using_s3:
            logger.debug("Connecting to s3")
            s3 = get_helper()

        i = 0
        j = 0
        missingfiles = []
        for dfid in query:
            df = session.query(DiskFile).get(dfid[0])
            if using_s3:
                logger.debug("Getting s3 key for %s" % df.filename)
                exists = s3.exists_key(df.filename)
            else:
                # Make it false if this one doesn't actually exist
                exists = df.exists()

            if(exists == False):
                df.present = False
                j += 1
                logger.info("File %d/%d: Marking file %s (diskfile id %d) as not present" % (i, n, df.filename, df.id))
                missingfiles.append(df.filename)
                session.commit()
            else:
                if ((i % 1000) == 0):
                    logger.info("File %d/%d: present and correct" % (i, n))
            i += 1

        if(j > 0):
            logger.warning("\nMarked %d files as no longer present\n%s\n" % (j, missingfiles))
        else:
            logger.info("\nMarked %d files as no longer present\n%s\n" % (j, missingfiles))

    logger.info("*** rollcall.py exiting normally at %s" % datetime.datetime.now())
