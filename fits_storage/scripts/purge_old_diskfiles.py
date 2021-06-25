import sys
import datetime

from gemini_obs_db import session_scope
from gemini_obs_db.file import File
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.header import Header
from gemini_obs_db.gmos import Gmos
from gemini_obs_db.niri import Niri

#from fits_storage_config import *
from fits_storage.logger import logger, setdebug, setdemon


if __name__ == "__main__":

    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Dry Run - do not actually do anything")
    parser.add_option("--file-pre", action="store", dest="filepre", help="filename prefix to elimanate non canonical diskfiles for")
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

    options, args = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Annouce startup
    logger.info("*********    purge_old_diskfiles.py - starting up at %s" % datetime.datetime.now())

    if not options.filepre:
        logger.error("You must specify a file-pre")
        sys.exit(1)

    if not options.dryrun:
        def delete(s, name, obj):
            logger.debug("Deleting {} id {}".format(name, obj.id))
            s.delete(obj)
            s.commit()
    else:
        def delete(s, name, obj):
            logger.debug("Dry Run - would delete {} id %d".format(name, obj.id))

    with session_scope() as session:
        logger.info("Getting list of file_ids to check")
        likestr = "%s%%" % options.filepre
        logger.debug("Matching File.name LIKE %s" % likestr)
        query = session.query(File.id).filter(File.name.like(likestr))
        fileids = query.all()

        logger.info("Got %d files to check" % len(fileids))

        if not fileids:
            session.close()
            logger.info("No files to check, exiting")
            sys.exit(0)

        # Loop through the file ids
        for fileid in fileids:
            #logger.debug("Checking file_id %d" % fileid)
            query = session.query(DiskFile).filter(DiskFile.file_id==fileid).filter(DiskFile.present==False).filter(DiskFile.canonical==False)
            todelete = query.all()
            if todelete:
                logger.info("Found diskfiles to delete for file_id %d" % fileid)
                for diskfile in todelete:
                    logger.debug("Need to delete diskfile id %d" % diskfile.id)
                    # Are there any header rows?
                    hquery = session.query(Header).filter(Header.diskfile_id==diskfile.id)
                    for header in hquery:
                        logger.debug("Need to delete header id %d" % header.id)
                        # Are there any instrument headers that need deleting?
                        gquery = session.query(Gmos).filter(Gmos.header_id==header.id)
                        for g in gquery:
                            delete(session, 'GMOS', g)
                        nquery = session.query(Niri).filter(Niri.header_id==header.id)
                        for n in nquery:
                            delete(session, 'NIRI', n)

                        delete(session, 'Header', header)
                    delete(session, 'Diskfile', diskfile)

    logger.info("*** purge_old_diskfiles exiting normally at %s" % datetime.datetime.now())

