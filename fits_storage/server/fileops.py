"""
This module provides the worker functions for the file operations queue.
These are called by the fileopser module.

These functions all take an 'args' argument which is a dictionary of actual
arguments, and also session and  logger arguments.

These functions can raise FileOpsError('message') to indicate a failure
that should be recorded in the fileopsqueue entry.
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


from fits_storage.server.fitseditor import FitsEditor

from fits_storage.config import get_config

if get_config().using_s3:
    from fits_storage.server.aws_s3 import Boto3Helper


class FileOpsError(Exception):
    """
    Worker functions can raise this exception with a string argument if they
    want to indicate failure.
    """
    pass


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
    fsc = get_config()
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

    fileuploadlog = session.get(FileUploadLog, fileuploadlog_id)

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
            s3 = Boto3Helper()
            extra_meta = misc_meta if it_is_misc else {}
            fileuploadlog.s3_ut_start = datetime.datetime.utcnow()
            fileuploadlog.s3_ok = s3.upload_file(dst, src, extra_meta) \
                                  is not None
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
                logger.debug("Copying %s to %s" % (srcmeta, dstmeta))
                shutil.move(srcmeta, dstmeta)
            # We can't use os.rename as that keeps the old permissions and
            # ownership, which we specifically want to avoid
            # Instead, we copy the file and the remove it.
            # Given that it's probably cross filesystem anyway, never mind.
            result = shutil.move(src, dst)
            if result != dst:
                logger.error("shutil.move failed, returned %s", result)
            fileuploadlog.file_ok = True
        except (IOError, OSError):
            logger.error("Error copying file to storage_root.", exc_info=True)
            return False

    # Queue it for ingest. We can pass no_defer=False here as even though we
    # know that the file is complete and ready to go, there's a race condition
    # where we cannot evaluate iqe.id to log it if it's already been ingested
    # and cleared from the queue by the time we ask for that.
    logger.info("Queueing %s for Ingest", filename)
    iq = IngestQueue(session, logger)
    iqe = iq.add(filename, path, no_defer=False)

    # iq.add returns None if the file is already on the queue
    if iqe is not None:
        fileuploadlog.ingestqueue_id = iqe.id
        session.commit()
    else:
        logger.info("File is already on queue, not added")
    return True


def update_headers(args, session, logger):
    """
    Update headers on a fits file. The args dictionary must contain either a
    'filename' or 'data_label' key to tell us which file to updates, and also
    can contain other keys, as follows:
    'qa_state': 'Pass', 'Fail', etc...
    'raw_site': 'iqany', etc. # How do we pass multiple values?
    'release': 'YYYY-MM-DD' release date
    'generic': {'KEYWORD1': 'value1', ...}
    'reject_new': Bool - if True, then refuse to insert new keywords.

    We return the md5sum of the updated fits file. This goes into the 'value'
    field of the FileOpsResponse instance and is used when we export a file
    using update headers to avoid retransferring the entire file. If there's
    an error, we raise FileOpsError.
    """

    logger.info("update_headers: %s", args)

    if 'filename' in args:
        logger.debug("Instantiating FitsEditor on filename %s",
                     args['filename'])
        fe = FitsEditor(filename=args['filename'],
                        session=session, logger=logger)
    elif 'data_label' in args:
        logger.debug("Instantiating FitsEditor on data_label %s",
                     args['data_label'])
        fe = FitsEditor(datalabel=args['data_label'],
                        session=session, logger=logger)
    else:
        logger.error('No Filename or data_label in update_header request')
        raise FileOpsError('No filename or data_label in update_header request')

    if fe.error is True:
        raise FileOpsError(f'Error instantiating FitsEditor: {fe.message}')

    if 'qa_state' in args:
        logger.debug("Updating qa_state: %s", args['qa_state'])
        fe.set_qa_state(args['qa_state'])
    if 'raw_site' in args:
        # TODO: Is this how multiple values are supposed to work?
        rawsites = args['raw_site'] if isinstance(args['raw_site'], list) \
            else [args['raw_site']]
        for rawsite in rawsites:
            logger.debug("Updating raw site: %s", rawsite)
            fe.set_rawsite(rawsite)
    if 'release' in args:
        logger.debug("Updating Release date: %s", args['release'])
        fe.set_release(args['release'])
    if 'generic' in args:
        reject_new = args.get('reject_new', False)
        if isinstance(args['generic'], dict):
            for keyword in args['generic']:
                value = args['generic'][keyword]
                logger.debug("Updating keyword: %s", keyword)
                fe.set_header(keyword, value, reject_new=reject_new)
        elif isinstance(args['generic'], list):
            for item in args['generic']:
                keyword, value = item
                logger.debug("Updating keyword: %s", keyword)
                fe.set_header(keyword, value, reject_new=reject_new)
        else:
            logger.error('Unknown format for generic headers args: %s',
                         args['generic'])

    filename = fe.diskfile.filename
    path = fe.diskfile.path
    fe.close()

    if fe.error:
        raise FileOpsError(fe.message)

    # Queue the file for ingest. Pass no_defer=True as we know the file is
    # complete and not still being modified
    iq = IngestQueue(session, logger)
    iqe=iq.add(filename, path, no_defer=True)
    if iqe:
        logger.info("Queued %s for Ingest", filename)
    else:
        logger.info("Queued %s for Ingest and got None - already on queue",
                    filename)
    session.commit()
