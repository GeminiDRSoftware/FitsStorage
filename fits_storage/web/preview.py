import datetime
import os

from sqlalchemy.exc import MultipleResultsFound, NoResultFound

from fits_storage.gemini_metadata_utils import gemini_fitsfilename

from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header
from fits_storage.server.orm.downloadlog import DownloadLog

from fits_storage.server.wsgi.context import get_context
from fits_storage.server.wsgi.returnobj import Return
from fits_storage.config import get_config
from fits_storage import utcnow

fsc = get_config()

if fsc.using_s3:
    from fits_storage.server.aws_s3 import Boto3Helper
    s3 = Boto3Helper()

from fits_storage.server.access_control_utils import icanhave


def preview(things):
    """
    This is the preview server, it sends you the preview jpg/png for the
    requested file. It handles authentication in that it won't give you the
    preview if you couldn't access the pixel data.

    things is a list of / separated things from the request, which could have
    just been a header_id
    """

    ctx = get_context()
    session = ctx.session

    header = None
    try:
        header_id = int(things[0])
        header = session.get(Header, header_id)
    except ValueError:
        pass

    if header is None:
        # Try and find by path and filename
        if len(things) == 0:
            return Return.HTTP_BAD_REQUEST
        elif len(things) == 1:
            filename = things[0]
            path = ''
        else:
            filename = things.pop(-1)
            path = '/'.join(things)

        try:
            header = session.query(Header).join(DiskFile).join(File) \
                .filter(DiskFile.canonical == True) \
                .filter(File.name == filename.removesuffix('.bz2')) \
                .filter(DiskFile.path == path) \
                .one()
        except MultipleResultsFound:
            ctx.resp.status = Return.HTTP_BAD_REQUEST
            return
        except NoResultFound:
            ctx.resp.status = Return.HTTP_NOT_FOUND
            return

        if header.diskfile.preview is None:
            # asking for a preview we do not have
            ctx.resp.status = Return.HTTP_NOT_FOUND
            return

    downloadlog = DownloadLog(ctx.usagelog)
    session.add(downloadlog)
    downloadlog.query_started = utcnow()

    try:
        # Is the client allowed to get this file?
        if icanhave(ctx, header):
            # Send them the data if we can
            sendpreview(header.diskfile.preview.filename)
        else:
            # Refuse to send data
            downloadlog.numdenied = 1
            ctx.resp.client_error(Return.HTTP_FORBIDDEN,
                                  "You don't have access to the requested data")
    finally:
        downloadlog.query_completed = utcnow()


def sendpreview(filename):
    """
    Send the one referenced preview file
    """

    resp = get_context().resp
    resp.set_header('Content-Disposition', f'inline; filename="{filename}"')

    if filename.endswith('.jpg'):
        resp.content_type = 'image/jpeg'
    elif filename.endswith('.png'):
        resp.content_type = 'image/png'

    # Send them the data
    if fsc.using_s3:
        # S3 file server
        keyname = f"{fsc.preview_path}/{filename}"
        flo = s3.get_flo(keyname)
        resp.append_iterable(flo)
    else:
        # Serve from regular file
        fullpath = os.path.join(fsc.storage_root, fsc.preview_path, filename)
        resp.sendfile(fullpath)
