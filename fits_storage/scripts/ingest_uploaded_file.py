import os
import sys
import traceback
import datetime
import shutil

from fits_storage.orm import session_scope
from fits_storage.orm.fileuploadlog import FileUploadLog, FileUploadWrapper

from fits_storage.fits_storage_config import storage_root, upload_staging_path, processed_cals_path, using_s3
from fits_storage.logger import logger, setdemon, setdebug
from fits_storage.utils.ingestqueue import IngestQueueUtil

if using_s3:
    from fits_storage.utils.aws_s3 import get_helper


"""
Ingest an uploaded file.

This can be used on the Archive to ingest a file that has been uploaded.
"""

if __name__ == "__main__":

    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--filename", action="store", type="string", dest="filename", help="filename of uploaded file to ingest")
    parser.add_option("--processed_cal", action="store", type="string", dest="processed_cal", help="Boolean, says whether file is a processed_cal")
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")
    parser.add_option("--fileuploadlog_id", action="store", type="int", dest="fileuploadlog_id", help="ID of FileUploadLog database entry to update")

    (options, args) = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Annouce startup
    now = datetime.datetime.now()
    logger.info("*********  ingest_uploaded_file.py - starting up at %s" % now)

    # Need a filename
    if not options.filename:
        logger.error("No filename specified, exiting")
        sys.exit(1)

    path = processed_cals_path if options.processed_cal == 'True' else ''

    with session_scope() as session:
        try:
            # The fileuploadwrapper acts as a Context Manager (to set times) and as a dummy
            # FileUploadLog, in case that we don't get an id. Makes the code simpler

            fileuploadlog = FileUploadWrapper()
            # Find the upload log entry to update
            if options.fileuploadlog_id:
                fileuploadlog.set_wrapped(session.query(FileUploadLog).get(options.fileuploadlog_id))

            # Move the file to its appropriate location in storage_root/path or S3
            # Construct the full path names and move the file into place
            src = os.path.join(upload_staging_path, options.filename)
            dst = os.path.join(path, options.filename)

            fileuploadlog.destination = dst

            if using_s3:
                # Copy to S3
                try:
                    logger.debug("Connecting to S3")
                    s3 = get_helper(logger_ = logger)
                    with fileuploadlog:
                        fileuploadlog.s3_ok = s3.upload_file(dst, src)
                    os.unlink(src)
                except:
                    string = traceback.format_tb(sys.exc_info()[2])
                    string = "".join(string)
                    fileuploadlog.add_note("Exception during S3 upload, see log file")
                    logger.error("Exception during S3 upload: %s : %s... %s" % (sys.exc_info()[0], sys.exc_info()[1], string))

            else:
                dst = os.path.join(storage_root, dst)
                logger.debug("Moving %s to %s" % (src, dst))
                # We can't use os.rename as that keeps the old permissions and ownership, which we specifically want to avoid
                # Instead, we copy the file and the remove it
                shutil.copy(src, dst)
                os.unlink(src)
                fileuploadlog.file_ok = True

            logger.info("Queueing for Ingest: %s" % dst)
            iq_id = IngestQueueUtil(session, logger).add_to_queue(options.filename, path)

            fileuploadlog.ingestqueue_id = iq_id

        except:
            string = traceback.format_tb(sys.exc_info()[2])
            string = "".join(string)
            logger.error("Exception: %s : %s... %s", sys.exc_info()[0], sys.exc_info()[1], string)
            raise

        finally:
            session.commit()

    logger.info("*** ingest_uploaded_file.py exiting normally at %s" % datetime.datetime.now())
