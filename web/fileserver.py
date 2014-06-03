from orm import sessionfactory

from fits_storage_config import odbkeypass, using_s3

from gemini_metadata_utils import gemini_fitsfilename

from orm.file import File
from orm.diskfile import DiskFile
from orm.header import Header
from orm.authentication import Authentication

# This will only work with apache
from mod_python import apache
from mod_python import Cookie
from mod_python import util

import time
import urllib
import re
import datetime
import gzip

if(using_s3):
    from boto.s3.connection import S3Connection
    from fits_storage_config import aws_access_key, aws_secret_key, s3_bucket_name

from utils.userprogram import icanhave

def fileserver(req, things):
    """
    This is the fileserver funciton. It always sends exactly one fits file, uncompressed.
    It handles authentication for serving the files too
    """

    # OK, first find the file they asked for in the database
    # tart up the filename if possible
    if(len(things) == 0):
        return apache.HTTP_NOT_FOUND
    filenamegiven = things.pop(0)
    filename = gemini_fitsfilename(filenamegiven)
    if(filename):
        pass
    else:
        filename = filenamegiven
    session = sessionfactory()
    try:
        query = session.query(File).filter(File.name == filename)
        if(query.count() == 0):
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

        if(canhaveit):
            # Send them the data
            sendonefile(req, header)
            return apache.OK
        else:
            # Refuse to send data
            return apache.HTTP_FORBIDDEN

    except IOError:
        pass
    finally:
        session.close()


def sendonefile(req, header):
    """
    Send the (one) fits file referred to by the header object to the client
    referred to by the req obect. This always sends unzipped data.
    """

    try:
        # Send them the data
        req.content_type = 'application/fits'
        req.headers_out['Content-Disposition'] = 'attachment; filename="%s"' % str(header.diskfile.file.name)
        if(using_s3):
            # S3 file server
            s3conn = S3Connection(aws_access_key, aws_secret_key)
            bucket = s3conn.get_bucket(s3_bucket_name)
            key = bucket.get_key(filename)
            req.set_content_length(header.diskfile.data_size)
            if(header.diskfile.gzipped):
                buffer = cStringIO.StringIO()
                key.get_contents_to_file(buffer)
                buffer.seek(0)
                gzfp = gzip.GzipFile(mode='rb', fileobj=buffer)
                try:
                    req.write(gzfp.read())
                finally:
                    gzfp.close()
                buffer.close()

            else:
                key.get_contents_to_file(req)
        else:
            # Serve from regular file
            if(header.diskfile.gzipped == True):
                # Unzip it on the fly
                req.set_content_length(header.diskfile.data_size)
                gzfp = gzip.open(header.diskfile.fullpath(), 'rb')
                try:
                    req.write(gzfp.read())
                finally:
                    gzfp.close()
            else:
                req.sendfile(diskfile.fullpath())

    except IOError:
        pass
