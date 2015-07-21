from orm import sessionfactory

from fits_storage_config import using_s3, storage_root, preview_path

from gemini_metadata_utils import gemini_fitsfilename

from orm.file import File
from orm.diskfile import DiskFile
from orm.header import Header
from orm.preview import Preview

from web.selection import getselection, openquery, selection_to_URL
from web.summary import list_headers
from web.user import userfromcookie

# This will only work with apache
from mod_python import apache
from mod_python import util

import os
import cStringIO

if using_s3:
    from boto.s3.connection import S3Connection
    from fits_storage_config import aws_access_key, aws_secret_key, s3_bucket_name

from utils.userprogram import icanhave

def preview(req, things):
    """
    This is the preview server, it sends you the preview jpg for the requested file.
    It handles authentication in that it won't give you the preview if you couldn't access
    the fits data.
    """

    # OK, first find the file they asked for in the database
    # tart up the filename if possible
    if len(things) == 0:
        return apache.HTTP_NOT_FOUND
    filenamegiven = things.pop(0)
    filename = gemini_fitsfilename(filenamegiven)
    if filename:
        pass
    else:
        filename = filenamegiven
    session = sessionfactory()
    try:
        query = session.query(File).filter(File.name == filename)
        if query.count() == 0:
            return apache.HTTP_NOT_FOUND
        file = query.one()
        # OK, we should have the file record now.
        # Next, find the canonical diskfile for it
        query = session.query(DiskFile).filter(DiskFile.present == True).filter(DiskFile.file_id == file.id)
        diskfile = query.one()
        # And now find the header record...
        query = session.query(Header).filter(Header.diskfile_id == diskfile.id)
        header = query.one()

        # Is the client allowed to get this file?
        canhaveit = icanhave(session, req, header)

        if canhaveit:
            # Send them the data if we can
            if sendpreview(session, req, diskfile.id):
                return apache.HTTP_OK
            else:
                return apache.HTTP_NOT_FOUND
        else:
            # Refuse to send data
            downloadlog.numdenied = 1
            return apache.HTTP_FORBIDDEN

    except IOError:
        pass
    finally:
        session.commit()
        session.close()


def sendpreview(session, req, diskfile_id):
    """
    Send the one preview file referenced by the diskfile_id
    Return True if we were able, False otherwise
    """

    # Find the preview entry
    query = session.query(Preview).filter(Preview.diskfile_id == diskfile_id)
    preview = query.first()
    if preview is None:
        return False
    
    # Send them the data
    req.content_type = 'image/jpeg'
    if using_s3:
        # S3 file server
        s3conn = S3Connection(aws_access_key, aws_secret_key)
        bucket = s3conn.get_bucket(s3_bucket_name)
        key = bucket.get_key(preview.filename)
        key.get_contents_to_file(req)
    else:
        # Serve from regular file
        fullpath = os.path.join(storage_root, preview_path, preview.filename)
        req.sendfile(fullpath)
    return True
