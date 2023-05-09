import os
import hashlib
import datetime

from fits_storage.server.wsgi.context import get_context
from fits_storage.server.wsgi.returnobj import Return

from fits_storage.server.orm.fileuploadlog import FileUploadLog

from .user import needs_login

from fits_storage.queues.queue.fileopsqueue import FileopsQueue, FileOpsRequest

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

    # Stream the data into the upload_staging file.
    # Calculate the md5 and size as we do it
    m = hashlib.md5()
    size = 0
    chunksize = 1000000  # 1MB
    fullfilename = os.path.join(fsc.upload_staging_dir, filename)
    # Content Length may or may not be defined. It's not required and if the
    # exporter is compressing on-the-fly, it won't know the length of the
    # compressed data ahead of time. Still, it's useful to log it.
    content_length = ctx.env['CONTENT_LENGTH']
    content_length = int(content_length) if content_length else None
    fileuploadlog.add_note(f"Content_Length header gave: {content_length}")
    # Python's wsgiref.simple_server has a bug with .read() that causes the
    # read to hang in certain situations. We can work around this if
    # content-length is set, but not if not. This is what bytes_left does.
    # https://github.com/python/cpython/issues/66077
    try:
        with open(fullfilename, 'wb') as f:
            fileuploadlog.ut_transfer_start = datetime.datetime.utcnow()
            while chunk := ctx.input.read(chunksize):
                size += len(chunk)
                m.update(chunk)
                f.write(chunk)
                # Work around simple_server bug if content_length is set.
                bytes_left = content_length - size if content_length else None
                if content_length and (bytes_left < chunksize):
                    chunksize = bytes_left
            fileuploadlog.ut_transfer_complete = datetime.datetime.utcnow()
    except IOError:
        fileuploadlog.add_note("IO Error writing upload_staging file")
        ctx.resp.client_error(Return.HTTP_INTERNAL_SERVER_ERROR,
                              "Could not store the file in the server")

    md5 = m.hexdigest()
    fileuploadlog.size = size
    fileuploadlog.md5 = md5

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
