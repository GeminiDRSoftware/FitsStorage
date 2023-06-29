import sys
import datetime
import os
from sqlalchemy import join, desc
import re

from fits_storage.fits_storage_config import storage_root
from fits_storage.logger import logger, setdebug, setdemon

from gemini_obs_db.db import session_scope
from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.file import File
from fits_storage.orm.tapestuff import Tape, TapeWrite, TapeFile

# Option Parsing
from optparse import OptionParser


if __name__ == "__main__":

    # Annouce startup
    logger.info("*********    tapeserver_delete_files.py - starting up at %s" % datetime.datetime.now())

    parser = OptionParser()
    parser.add_option("--file-pre", action="store", type="string", dest="filepre", help="File prefix to operate on, eg N20090130, N200812 etc")
    parser.add_option("--tapeset", action="append", type="int", dest="tapeset", help="Tape set number to check file is on. Can be given multiple times")
    parser.add_option("--maxnum", type="int", action="store", dest="maxnum", help="Delete at most X files.")
    parser.add_option("--skip-md5", action="store_true", dest="skipmd5", help="Do not bother to verify the md5 of the file on disk")
    parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Dry Run - do not actually do anything")
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

    (options, args) = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    if options.tapeset is None:
        logger.error("Must supply a tapeset to use this")
        exit(1)

    with session_scope() as session:
        query = session.query(DiskFile).filter(DiskFile.present==True)

        if options.filepre:
            likestr = "%s%%" % options.filepre
            query = query.filter(DiskFile.filename.like(likestr))

            query = query.order_by(DiskFile.filename)

        if options.maxnum:
            query = query.limit(options.maxnum)
        cnt = query.count()

        if cnt == 0:
            logger.info("No Files found matching file-pre. Exiting")
            sys.exit(0)

        logger.info("Got %d files to consider for deletion" % cnt)

        numdel = 0
        numskip = 0

        for diskfile in query:
            badmd5 = False

            fullpath = diskfile.fullpath()
            dbmd5 = diskfile.file_md5
            dbdatamd5 = diskfile.data_md5
            dbfilename = diskfile.filename

            logger.debug("Full path filename: %s" % fullpath)
            if not diskfile.exists():
                logger.error("Cannot access file %s" % fullpath)
            else:
                if not options.skipmd5:
                    filemd5 = diskfile.get_file_md5()
                    logger.debug("Actual File MD5 and canonical database diskfile MD5 are: %s and %s" % (filemd5, dbmd5))
                    if filemd5 != dbmd5:
                        logger.error("File: %s has an md5sum mismatch between the database and the actual file. Skipping" % dbfilename)
                        badmd5 = True
                else:
                    filemd5 = dbmd5

                if not badmd5:
                    ontape = True
                    for ts in options.tapeset:
                        tq = session.query(TapeFile).select_from(join(join(TapeFile, TapeWrite), Tape))
                        tq = tq.filter(TapeFile.filename == dbfilename).filter(TapeFile.md5 == dbmd5)
                        tq = tq.filter(Tape.set == ts)
                        tq = tq.filter(TapeWrite.suceeded == True).filter(Tape.active == True)
                        tfs = tq.all()

                        logger.debug("Found %d tapefiles for %s on tapeset %d", len(tfs), dbfilename, ts)

                        if len(tfs) == 0:
                            logger.info("%s not found on tapeset %d - skipping", dbfilename, ts)
                            ontape = False
                            break
                    if ontape:
                        if options.dryrun:
                            logger.info("Dry run - not actually deleting %s", dbfilename)
                        else:
                            try:
                                logger.info("Deleting %s", dbfilename)
                                os.unlink(fullpath)
                                logger.debug("Marking diskfile id %d as not present" % diskfile.id)
                                diskfile.present = False
                                session.commit()
                                numdel += 1
                            except:
                                logger.error("Error deleting %s", fullpath)
                    else:
                        numskip += 1

    if options.dryrun:
        logger.info("Number of files not actually deleted = %d, number skipped = %d", numdel, numskip)
    else:
        logger.info("Number of files deleted = %d, number skipped = %d", numdel, numskip)
    logger.info("** tapeserver_delete_files.py exiting normally")
