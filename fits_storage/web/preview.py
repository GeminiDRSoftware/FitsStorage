from ..fits_storage_config import using_s3, storage_root, preview_path

from ..gemini_metadata_utils import gemini_fitsfilename

from gemini_obs_db.file import File
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.header import Header
from gemini_obs_db.preview import Preview
from ..orm.downloadlog import DownloadLog

from ..utils.web import get_context, Return, with_content_type

from .selection import getselection, openquery, selection_to_URL

import datetime
import os

if using_s3:
    from ..utils.aws_s3 import get_helper
    s3 = get_helper()

from ..utils.userprogram import icanhave


def num_previews(filenamegiven):
    """
    This is the preview server, it sends you the preview jpg for the requested file.
    It handles authentication in that it won't give you the preview if you couldn't access
    the fits data.
    """

    ctx = get_context()

    # OK, first find the file they asked for in the database
    # tart up the filename if possible
    filename = gemini_fitsfilename(filenamegiven)
    if not filename:
        filename = filenamegiven

    session = ctx.session

    header = None

    try:
        diskfile = \
            session.query(DiskFile) \
                .join(File, DiskFile.file_id == File.id) \
                .filter(DiskFile.canonical == True) \
                .filter(File.name == filename) \
                .first()
        ctx.resp.set_content_type('text/plain')
        ctx.resp.append("%d" % len(diskfile.previews))
        return
    except TypeError: # Will happen if .first() returns None
        ctx.resp.status = Return.HTTP_NOT_FOUND
        return


def preview(filenamegiven, number=0):
    """
    This is the preview server, it sends you the preview jpg for the requested file.
    It handles authentication in that it won't give you the preview if you couldn't access
    the fits data.
    """

    ctx = get_context()

    # OK, first find the file they asked for in the database
    # tart up the filename if possible
    filename = gemini_fitsfilename(filenamegiven)
    if not filename:
        filename = filenamegiven

    session = ctx.session

    header = None
    preview = None

    if number is None:
        number = 0
    try:
        header = \
            session.query(Header) \
                .join(DiskFile, Header.diskfile_id == DiskFile.id) \
                .join(File, DiskFile.file_id == File.id) \
                .filter(DiskFile.canonical == True) \
                .filter(File.name == filename) \
                .first()
        diskfile = header.diskfile
        preview = diskfile.previews[number]
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
            ctx.resp.client_error(Return.HTTP_FORBIDDEN, "You don't have access to the requested data")
    finally:
        downloadlog.query_completed = datetime.datetime.utcnow()


@with_content_type('image/jpeg')
def sendpreview(preview):
    """
    Send the one referenced preview file
    """

    resp = get_context().resp

    # Send them the data
    if using_s3:
        # S3 file server
        with s3.fetch_temporary(preview.filename, skip_tests=True) as temp:
            resp.append_iterable(temp)
    else:
        # Serve from regular file
        fullpath = os.path.join(storage_root, preview_path, preview.filename)
        resp.sendfile(fullpath)
