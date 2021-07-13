from gemini_obs_db.db import session_scope
from gemini_obs_db.orm.diskfile import DiskFile

from fits_storage.logger import logger, setdebug

from sqlalchemy import desc
import datetime
from optparse import OptionParser

from fits_storage.fits_storage_config import using_s3, storage_root
from os.path import basename
from glob import iglob


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--path", action="store", type="string", dest="path", help="Path within /sci/dataflow")
    parser.add_option("--filename-pre", action="store", type="string", dest="filepre", help="Filename prefix to filter on")

    (options, args) = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)


    path = options.path
    if path is None:
        path = ""
    filepre = options.filepre
    if not filepre:
        print("--filename-pre is required, we use it to scan the archive contents efficiently")
    if len(filepre) < 6:
        print("Minimum 6 character filename-pre length")

    # Annouce startup
    logger.info("*********    verify_in_diskfiles.py - starting up at %s" % datetime.datetime.now())
    print("*********    verify_in_diskfiles.py - starting up at %s" % datetime.datetime.now())

    if using_s3:
        logger.warning("Not compatible with S3, exiting")
        exit(1)

    # Get a database session
    with session_scope() as session:
        count = 0

        if path:
            filenames = iglob("%s/%s/%s*.fits*" % (storage_root, path, filepre))
        else:
            filenames = iglob("%s/%s*.fits*" % (storage_root, filepre))

        filechecks = dict()

        for filename in filenames:
            basefilename = basename(filename)
            # Get a list of all diskfile_ids marked as present
            if path != '':
                query = session.query(DiskFile) \
                    .filter(DiskFile.path == path).filter(DiskFile.canonical == True). \
                    filter(DiskFile.filename == basefilename).order_by(desc(DiskFile.lastmod))
            else:
                query = session.query(DiskFile) \
                    .filter(DiskFile.canonical == True). \
                    filter(DiskFile.filename == basefilename).order_by(desc(DiskFile.lastmod))

            record = query.first()

            if record is None:
                print(filename)

    logger.info("*** verify_diskfiles.py exiting normally at %s" % datetime.datetime.now())
