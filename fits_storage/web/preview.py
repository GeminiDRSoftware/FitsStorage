import datetime
import os

from fits_storage.gemini_metadata_utils import gemini_fitsfilename

from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header
from fits_storage.server.orm.downloadlog import DownloadLog

from fits_storage.server.wsgi.context import get_context
from fits_storage.server.wsgi.returnobj import Return
from fits_storage.config import get_config

fsc = get_config()

if fsc.using_s3:
    from fits_storage.server.aws_s3 import Boto3Helper
    s3 = Boto3Helper()

from fits_storage.server.access_control_utils import icanhave


def preview(filenamegiven):
    """
    This is the preview server, it sends you the preview jpg for the
    requested file. It handles authentication in that it won't give you the
    preview if you couldn't access the pixel data.
    """

    ctx = get_context()

    # OK, first find the file they asked for in the database
    # tart up the filename if possible
    filename = gemini_fitsfilename(filenamegiven)
    if not filename:
        filename = filenamegiven

    session = ctx.session

    try:
        header = session.query(Header)\
            .join(DiskFile, Header.diskfile_id == DiskFile.id)\
            .join(File, DiskFile.file_id == File.id)\
            .filter(DiskFile.canonical == True)\
            .filter(File.name == filename)\
            .first()
        diskfile = header.diskfile
        if diskfile.preview is None:
            # asking for a preview we do not have
            ctx.resp.status = Return.HTTP_NOT_FOUND
            return
    except TypeError:  # Will happen if .first() returns None
        ctx.resp.status = Return.HTTP_NOT_FOUND
        return

    downloadlog = DownloadLog(ctx.usagelog)
    session.add(downloadlog)
    downloadlog.query_started = datetime.datetime.utcnow()

    try:
        # Is the client allowed to get this file?
        if icanhave(ctx, header):
            # Send them the data if we can
            sendpreview(diskfile.preview.filename)
        else:
            # Refuse to send data
            downloadlog.numdenied = 1
            ctx.resp.client_error(Return.HTTP_FORBIDDEN,
                                  "You don't have access to the requested data")
    finally:
        downloadlog.query_completed = datetime.datetime.utcnow()


def sendpreview(filename):
    """
    Send the one referenced preview file
    """

    resp = get_context().resp
    resp.content_type = 'image/jpeg'

    # Send them the data
    if fsc.using_s3:
        # S3 file server
        with s3.fetch_temporary(filename, skip_tests=True) as temp:
            resp.append_iterable(temp)
    else:
        # Serve from regular file
        fullpath = os.path.join(fsc.storage_root, fsc.preview_path, filename)
        resp.sendfile(fullpath)
