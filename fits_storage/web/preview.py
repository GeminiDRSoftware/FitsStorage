from ..orm import session_scope

from ..fits_storage_config import using_s3, storage_root, preview_path

from ..gemini_metadata_utils import gemini_fitsfilename

from ..orm.file import File
from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.preview import Preview
from ..orm.downloadlog import DownloadLog

from ..utils.web import Context

from .selection import getselection, openquery, selection_to_URL
from .summary import list_headers
from .user import userfromcookie, AccessForbidden

# This will only work with apache
from mod_python import apache
from mod_python import util

import datetime
import os

if using_s3:
    from ..utils.aws_s3 import get_helper
    s3 = get_helper()

from ..utils.userprogram import icanhave

def preview(req, things):
    """
    This is the preview server, it sends you the preview jpg for the requested file.
    It handles authentication in that it won't give you the preview if you couldn't access
    the fits data.
    """

    # OK, first find the file they asked for in the database
    # tart up the filename if possible
    try:
        filenamegiven = things.pop(0)
        filename = gemini_fitsfilename(filenamegiven)
        if filename:
            pass
        else:
            filename = filenamegiven
    except IndexError:
        return apache.HTTP_NOT_FOUND

    with session_scope() as session:
        try:
            # Find the information associated with the canonical diskfile and header for the file on the query
            preview, header = (
                session.query(Preview, Header).join(DiskFile).join(Header).join(File)
                        .filter(DiskFile.present == True)
                        .filter(File.name == filename)
                        .first()
                )
        except TypeError: # Will happen if .first() returns None
            return apache.HTTP_NOT_FOUND

        downloadlog = DownloadLog(Context().usagelog)
        session.add(downloadlog)
        downloadlog.query_started = datetime.datetime.utcnow()

        try:
            # Is the client allowed to get this file?
            if icanhave(session, req, header):
                # Send them the data if we can
                sendpreview(req, preview)
            else:
                # Refuse to send data
                downloadlog.numdenied = 1
                raise AccessForbidden("You don't have access to the requested data")
        finally:
            downloadlog.query_completed = datetime.datetime.utcnow()

        return apache.HTTP_OK

def sendpreview(req, preview):
    """
    Send the one referenced preview file
    """

    # Send them the data
    req.content_type = 'image/jpeg'
    if using_s3:
        # S3 file server
        with s3.fetch_temporary(preview.filename, skip_tests=True) as temp:
            req.write(temp.read())
    else:
        # Serve from regular file
        fullpath = os.path.join(storage_root, preview_path, preview.filename)
        req.sendfile(fullpath)
