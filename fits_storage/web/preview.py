from ..fits_storage_config import using_s3, storage_root, preview_path

from ..gemini_metadata_utils import gemini_fitsfilename

from ..orm.file import File
from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.preview import Preview
from ..orm.downloadlog import DownloadLog

from ..utils.web import Context, Return, with_content_type

from .selection import getselection, openquery, selection_to_URL
from .user import AccessForbidden

import datetime
import os

if using_s3:
    from ..utils.aws_s3 import get_helper
    s3 = get_helper()

from ..utils.userprogram import icanhave

def preview(filenamegiven):
    """
    This is the preview server, it sends you the preview jpg for the requested file.
    It handles authentication in that it won't give you the preview if you couldn't access
    the fits data.
    """

    ctx = Context()

#    try:
#        filenamegiven = things.pop(0)
#    except IndexError:
#        ctx.resp.status = Return.HTTP_NOT_FOUND
#        return

    # OK, first find the file they asked for in the database
    # tart up the filename if possible
    filename = gemini_fitsfilename(filenamegiven)
    if not filename:
        filename = filenamegiven

    session = ctx.session

    try:
        # Find the information associated with the canonical diskfile and header for the file on the query
        preview, header = (
            session.query(Preview, Header).join(DiskFile).join(Header).join(File)
                    .filter(DiskFile.present == True)
                    .filter(File.name == filename)
                    .first()
            )
    except TypeError: # Will happen if .first() returns None
        ctx.resp.status = Return.HTTP_NOT_FOUND
        return

    downloadlog = DownloadLog(ctx.usagelog)
    session.add(downloadlog)
    downloadlog.query_started = datetime.datetime.utcnow()

    try:
        # Is the client allowed to get this file?
        if icanhave(ctx, header):
            # Send them the data if we can
            sendpreview(preview)
        else:
            # Refuse to send data
            downloadlog.numdenied = 1
            raise AccessForbidden("You don't have access to the requested data")
    finally:
        downloadlog.query_completed = datetime.datetime.utcnow()

@with_content_type('image/jpeg')
def sendpreview(preview):
    """
    Send the one referenced preview file
    """

    resp = Context().resp

    # Send them the data
    if using_s3:
        # S3 file server
        with s3.fetch_temporary(preview.filename, skip_tests=True) as temp:
            resp.append_iterable(temp)
    else:
        # Serve from regular file
        fullpath = os.path.join(storage_root, preview_path, preview.filename)
        resp.sendfile(fullpath)
