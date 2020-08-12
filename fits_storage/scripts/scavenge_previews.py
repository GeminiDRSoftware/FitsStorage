from fits_storage.orm import session_scope
from fits_storage.fits_storage_config import storage_root, using_s3
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.orm.preview import Preview
from fits_storage.orm.diskfile import DiskFile
import os
import re
import datetime
import time
if using_s3:
    from fits_storage.utils.aws_s3 import get_helper
    s3 = get_helper()

"""
Helper script to ensure we have `~Preview` records for any `_preview.jpg` files.

The script will validate that we have a corresponding `~DiskFile` for the `~Preview`,
create the `~Preview` and then link them.
"""
if __name__ == "__main__":

    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--file-re", action="store", type="string", dest="file_re", help="python regular expression string to select files by.")
    parser.add_option("--path", action="store", dest="path", help="Path to directory in storage_root")
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

    options, args = parser.parse_args()
    path = options.path

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Annouce startup
    now = datetime.datetime.now()
    logger.info("*********    scavenge_previews.py - starting up at %s" % now)

    # Get a list of all the files in the datastore
    # We assume this is just one dir (ie non recursive) for now.

    if using_s3:
        logger.info("Querying files for ingest from S3 bucket")
        filelist = s3.key_names()
    else:
        fulldirpath = os.path.join(storage_root, path)
        logger.info("Queueing files for ingest from: %s" % fulldirpath)
        filelist = os.listdir(fulldirpath)

    logger.info("Got file list. Got %d files" % len(filelist))

    file_re = options.file_re

    files = []
    if file_re:
        cre = re.compile(file_re)
        files = filter(cre.search, filelist)
    else:
        files = filelist

    # Find only the preview files
    thefiles = []
    previewcre = re.compile("_preview.jpg")
    for filename in files:
        if previewcre.search(filename):
            thefiles.append(filename)

    n = len(thefiles)
    # print what we're about to do, and give abort opportunity
    logger.info("About to scan %d files" % n)
    if n > 5000:
        logger.info("That's a lot of files. Hit ctrl-c within 5 secs to abort")
        time.sleep(6)

    i=0
    with session_scope() as session:
        # Loop through the previewfile found
        for previewfile in thefiles:
            # Is it already in the preview table
            p = session.query(Preview).filter(Preview.filename == previewfile).first()
            if p:
                # Already have it
                logger.debug("Already have a preview entry for %s", previewfile)
            else:
                # Not in preview table
                # Construct the diskfile filename
                dffn = previewfile[:-12]
                # Find the diskfile for that
                dfs = session.query(DiskFile).filter(DiskFile.filename==dffn).filter(DiskFile.canonical==True).all()
                if len(dfs) == 0:
                    logger.error("Preview %s has no canonical diskfile named %s" % (previewfile, dffn))
                elif len(dfs) > 1:
                    logger.error("Preview %s has multiple canonical diskfiles named %s" % (previewfile, dffn))
                else:
                    df = dfs[0]
                    logger.info("Adding Preview %s <-> Diskfile id %d (diskfilename %s)" % (previewfile, df.id, df.filename))
                    p = Preview(df, previewfile)
                    session.add(p)
                    if (i % 5000) == 0:
                        logger.info("Comitting")
                        session.commit()
            i += 1
        logger.info("Comitting")
        session.commit()

    logger.info("*** scavenge_previews exiting normally at %s" % datetime.datetime.now())
