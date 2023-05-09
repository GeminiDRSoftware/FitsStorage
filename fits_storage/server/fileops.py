"""
This module provides the worker functions for the file operations queue.
These are called by the fileopser module.

These functions all take an 'args' argument which is a dictionary of actual
arguments, and also session and  logger arguments.
"""

import os.path
import datetime
import traceback
import sys
import shutil

from fits_storage.server.orm.fileuploadlog import FileUploadLog
from fits_storage.queues.queue.ingestqueue import IngestQueue

from fits_storage.server.orm.miscfile import is_miscfile, miscfile_meta, \
    miscfile_meta_path

from fits_storage.server.aws_s3 import get_helper

from fits_storage.config import get_config
fsc = get_config()


def echo(args, session, logger):
    """
    Simple echo function, used for testing the response_required functionality
    """
    return args['echo']


def ingest_upload(args, session, logger):
    """
    Ingest a file that has been uploaded. Either copy it from the upload_staging
    directory to the storage_root (taking care of ownership and permissions),
    or upload it to s3 if we are using_s3. Then add it to the ingest queue.

    Return True on success, False on failure.
    """

    try:
        filename = args['filename']
        fileuploadlog_id = args['fileuploadlog_id']
        processed_cal = args['processed_cal']
    except KeyError:
        logger.error("Missing critical arguments in ingest_upload",
                     exc_info=True)
        return False

    logger.info("ingest_upload: filename: %s, fileuploadlog_id: %s, "
                "processed_cal: %s", filename, fileuploadlog_id, processed_cal)

    fileuploadlog = session.query(FileUploadLog).get(fileuploadlog_id)

    # Move the file to its appropriate location in storage_root/path or S3

    # Construct the full path names and move the file into place
    path = 'processed_cals' if processed_cal else ''
    src = os.path.join(fsc.upload_staging_dir, filename)
    dst = os.path.join(path, filename)
    fileuploadlog.destination = dst

    it_is_misc = is_miscfile(src)
    try:
        if it_is_misc:
            misc_meta = miscfile_meta(src, urlencode=True)
    except Exception as e:
        logger.error("Exception calling miscfile_meta: ", exc_info=True)
        return False

    if fsc.using_s3:
        logger.debug("Copying to S3")
        try:
            s3 = get_helper()
            extra_meta = misc_meta if it_is_misc else {}
            fileuploadlog.s3_ut_start = datetime.datetime.utcnow()
            fileuploadlog.s3_ok = s3.upload_file(dst, src, extra_meta)
            fileuploadlog.s3_ut_end = datetime.datetime.utcnow()
            os.unlink(src)
            if it_is_misc:
                os.unlink(miscfile_meta_path(src))
            logger.debug("Copy to S3 appeared to work")
        except Exception:
            string = traceback.format_tb(sys.exc_info()[2])
            string = "".join(string)
            fileuploadlog.add_note("Exception during S3 upload:\n%s\n"
                                   % string)
            session.commit()
            logger.error("Exception during S3 upload.", exc_info=True)
            return False
    else:
        try:
            dst = os.path.join(fsc.storage_root, dst)
            logger.debug("Moving %s to %s" % (src, dst))
            if is_miscfile(src):
                srcmeta = miscfile_meta_path(src)
                dstmeta = miscfile_meta_path(dst)
                shutil.copy(srcmeta, dstmeta)
            # We can't use os.rename as that keeps the old permissions and
            # ownership, which we specifically want to avoid
            # Instead, we copy the file and the remove it.
            # TODO - can we os.rename then os.chown and os.chmod ?
            # Given that it's probably cross filesystem anyway, never mind.
            shutil.copy(src, dst)
            os.unlink(src)
            fileuploadlog.file_ok = True
        except (IOError, OSError):
            logger.error("Error copying file to storage_root.", exc_info=True)
            return False

    logger.info("Queueing %s for Ingest", filename)
    iq = IngestQueue(session, logger)
    iqe = iq.add(filename, path)

    fileuploadlog.ingestqueue_id = iqe.id
    session.commit()
    return True
