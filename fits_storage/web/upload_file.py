import os
import hashlib
import datetime
import errno

from fits_storage.server.wsgi.context import get_context
from fits_storage.server.wsgi.returnobj import Return

from fits_storage.server.orm.fileuploadlog import FileUploadLog

from .user import needs_login

from fits_storage.queues.queue.fileopsqueue import FileopsQueue, \
    FileOpsResponse, FileOpsRequest

from fits_storage.logger import DummyLogger

from fits_storage.config import get_config
fsc = get_config()


@needs_login(only_magic=True, magic_cookies=[
    ('gemini_fits_upload_auth', fsc.upload_auth_cookie)],
             annotate=FileUploadLog)
def upload_file(filename, processed_cal=False):
    """
    This handles uploading files, including processed calibrations.
    It has to be called via a POST request with a binary data payload

    If upload authentication is enabled, the request must contain
    the authentication cookie for the request to be processed.

    Log Entries are inserted into the FileUploadLog table
    """

    ctx = get_context()

    session = ctx.session

    fileuploadlog = FileUploadLog(ctx.usagelog)
    fileuploadlog.filename = filename
    fileuploadlog.processed_cal = processed_cal
    session.add(fileuploadlog)
    session.commit()

    if ctx.env.method != 'POST':
        fileuploadlog.add_note("Aborted - not HTTP POST")
        ctx.resp.status = Return.HTTP_NOT_ACCEPTABLE
        return

    # It's brute force to read all the data in one chunk,
    # but that's fine, files are never more than a few hundred MB...
    fileuploadlog.ut_transfer_start = datetime.datetime.utcnow()
    clientdata = ctx.raw_data

    fileuploadlog.ut_transfer_complete = datetime.datetime.utcnow()
    fullfilename = os.path.join(fsc.upload_staging_dir, filename)

    try:
        with open(fullfilename, 'wb') as f:
            f.write(clientdata)
    except IOError as e:
        if e.errno in (errno.EPERM, errno.EACCES):
            ctx.resp.client_error(Return.HTTP_FORBIDDEN,
                                  "Could not store the file in the server "
                                  "due to lack of permissions")
        elif e.errno == errno.ENOENT:
            ctx.resp.client_error(Return.HTTP_NOT_FOUND,
                                  "Could not store the file. "
                                  "The staging directory seems to be missing")

    # compute the md5 and size while we still have the buffer in memory
    m = hashlib.md5()
    m.update(clientdata)
    md5 = m.hexdigest()
    size = len(clientdata)
    fileuploadlog.size = size
    fileuploadlog.md5 = md5

    # Free up memory
    clientdata = None

    # Construct the verification dictionary and json encode it
    verification = {'filename': filename, 'size': size, 'md5': md5}
    # And write that back to the client
    ctx.resp.append_json([verification])

    # Put the ingest_upload request on the fileops queue. We trust fileops to
    # do its thing asynchronously and do not wait for a response here.
    fq = FileopsQueue(session, logger=DummyLogger())

    fo_req = FileOpsRequest(request="ingest_upload",
                            args={"filename": filename,
                                  "processed_cal": processed_cal,
                                  "fileuploadlog_id": fileuploadlog.id})

    fq.add(fo_req, filename=filename, response_required=False)


