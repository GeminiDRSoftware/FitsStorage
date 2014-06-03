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

if(using_s3):
    from boto.s3.connection import S3Connection
    from fits_storage_config import aws_access_key, aws_secret_key, s3_bucket_name

def fileserver(req, things):
    """
    This is the fileserver funciton. It handles authentication for serving the files too
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

        # OK, now figure out if the data are public
        today = datetime.datetime.utcnow().date()
        canhaveit = False

        # Are we passed the release data?
        if((header.release) and (today >= header.release)):
            # Yes, the data are public
            canhaveit = True

        # Is the data a dayCal or a partnerCal or an acqCal?
        elif(header.observation_class in ['dayCal', 'partnerCal', 'acqCal']):
            # Yes, the data are public. These should have a release date too, except that
            # Cals from the pipeline processed directly off the DHS machine don't
            canhaveit = True

        else:
            # No, the data are not public. See if we got the magic cookie
            cookies = Cookie.get_cookies(req)
            if(cookies.has_key('gemini_fits_authorization')):
                auth = cookies['gemini_fits_authorization'].value
                if(auth == 'good_to_go'):
                    # OK, we got the magic cookie
                    canhaveit = True

        # Did we get a program ID authentication cooke?
        cookie_key = None
        # Is this program ID in the authentication table? If so, what's the key?
        program_key = None
        program_id = header.program_id
        authquery = session.query(Authentication).filter(Authentication.program_id == program_id)
        if(authquery.count() == 1):
            auth = authquery.one()
            program_key = auth.program_key
        cookies = Cookie.get_cookies(req)
        if(cookies.has_key(program_id)):
            cookie_key = cookies[program_id].value
        if((program_key is not None) and (program_key == cookie_key)):
            canhaveit = True

        if(canhaveit):
            # Send them the data
            req.content_type = 'application/fits'
            req.headers_out['Content-Disposition'] = 'attachment; filename="%s"' % filename
            if(using_s3):
                # S3 file server
                # For now, just serve what we have.
                # Need to implement gz and non gz requests somehow
                s3conn = S3Connection(aws_access_key, aws_secret_key)
                bucket = s3conn.get_bucket(s3_bucket_name)
                key = bucket.get_key(filename)
                req.set_content_length(diskfile.file_size)
                key.get_contents_to_file(req)
            else:
                # Serve from regular file
                if(diskfile.gzipped == True):
                    # Unzip it on the fly
                    req.set_content_length(diskfile.data_size)
                    gzfp = gzip.open(diskfile.fullpath(), 'rb')
                    try:
                        req.write(gzfp.read())
                    finally:
                        gzfp.close()
                else:
                    req.sendfile(diskfile.fullpath())

            return apache.OK
        else:
            # Refuse to send data
            return apache.HTTP_FORBIDDEN

    except IOError:
        pass
    finally:
        session.close()

