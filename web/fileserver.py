from orm import sessionfactory

from fits_storage_config import odbkeypass, using_s3, fits_open_result_limit, fits_closed_result_limit

from gemini_metadata_utils import gemini_fitsfilename

from orm.file import File
from orm.diskfile import DiskFile
from orm.header import Header
from orm.authentication import Authentication

from web.selection import getselection, openquery, selection_to_URL
from web.summary import list_headers
from web.user import userfromcookie

# This will only work with apache
from mod_python import apache
from mod_python import Cookie
from mod_python import util

import time
import urllib
import re
import datetime
import gzip
import cStringIO
import tarfile

if(using_s3):
    from boto.s3.connection import S3Connection
    from fits_storage_config import aws_access_key, aws_secret_key, s3_bucket_name

from utils.userprogram import icanhave

def download(req, things):
    """
    This is the download server. Given a selection, it will send a tarball of the 
    files from the selection that you have access to to the client. 
    """
    # Get the selection
    selection = getselection(things)

    # Open a database session
    session = sessionfactory()
    try:
        # Get our username while we have the database session open
        user = userfromcookie(session, req)
        if(user):
            username = user.username
        else:
            username = 'Not Logged In'

        # Get the header list
        headers = list_headers(session, selection, None)

        if(openquery(selection) and len(headers) > fits_open_result_limit):
            # Open query. Almost certainly too many files
            req.content_type = "text/plain"
            req.write("Your selection criteria does not restrict the number of results, and more than %d were found. " % fits_open_result_limit)
            req.write("Please refine your selection more before attempting to download. Queries that can contain an arbitrary number of results have a lower limit applied than more constrained queries. Including a date range or program id will prevent an arbitrary number of results being found will raise the limit")
            return apache.OK

        if(len(headers) > fits_closed_result_limit):
            # Open query. Almost certainly too many files
            req.content_type = "text/plain"
            req.write("More than %d results were found. This is beyond the limit we allow" % fits_closed_result_limit)
            req.write("Please refine your selection more before attempting to download. If you really want all these files, we suggest you break your search into several smaller date range pieces and download one set at a time.")
            return apache.OK

        # Set up the http headers
        req.content_type = "application/tar"
        req.headers_out['Content-Disposition'] = 'attachment; filename="download.tar"'

        if(using_s3):
            s3conn = S3Connection(aws_access_key, aws_secret_key)
            bucket = s3conn.get_bucket(s3_bucket_name)

        # We are going to build an md5sum file while we do this
        md5file = ""
        # And keep a list of any files we were denied
        denied = []
        # Here goes!
        tar = tarfile.open(name="download.tar", mode="w|", fileobj=req)
        for header in headers:
            if(icanhave(session, req, header)):
                md5file += "%s  %s\n" % (header.diskfile.file_md5, header.diskfile.filename)
                if(using_s3):
                    # Fetch the file into a cStringIO buffer
                    key = bucket.get_key(header.diskfile.filename)
                    buffer = cStringIO.StringIO()
                    key.get_contents_to_file(buffer)
                    # Write buffer into tarfile
                    buffer.seek(0)
                    # - create a tarinfo object
                    tarinfo = tarfile.TarInfo(header.diskfile.filename)
                    tarinfo.size = header.diskfile.file_size
                    tarinfo.uid = 0
                    tarinfo.gid = 0
                    tarinfo.uname = 'gemini'
                    tarinfo.gname = 'gemini'
                    tarinfo.mtime = time.mktime(header.diskfile.lastmod.timetuple())
                    tarinfo.mode = 0644
                    # - and add it to the tar file
                    tar.addfile(tarinfo, buffer)
                    buffer.close()
                else:
                    tar.add(header.diskfile.fullpath(), header.diskfile.filename)
            else:
                # Permission denied, add to the denied list
                denied.append(header.diskfile.filename)
        # OK, that's all the fits files. Add the md5sum file
        # - create a tarinfo object
        tarinfo = tarfile.TarInfo('md5sums.txt')
        tarinfo.size = len(md5file)
        tarinfo.uid = 0
        tarinfo.gid = 0
        tarinfo.uname = 'gemini'
        tarinfo.gname = 'gemini'
        tarinfo.mtime = time.time()
        tarinfo.mode = 0644
        # - and add it to the tar file
        buffer = cStringIO.StringIO(md5file)
        tar.addfile(tarinfo, buffer)
        buffer.close()

        # And add the README.TXT file
        readme = "This is a tar file of search results downloaded from the gemini archive.\n\n"
        readme += "The search criteria was: %s\n" % selection_to_URL(selection)
        readme += "The search was performed at: %s UTC\n" % datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        readme += "The search was performed by archive user: %s\n\n" % username
        readme += "We have included a file listing the md5sums of the data files in here.\n"
        readme += "If you have the 'md5sum' utility installed (most Linux machines at least),\n"
        readme += "You can verify file integrity by running 'md5sum -c md5sums.txt'.\n\n"
        if(denied):
            readme += "The following files in your search results were not included,\n"
            readme += "because they are proprietary data that you do not have access to:\n"
            readme += '\n'.join(denied) + '\n'
        # - create a tarinfo object
        tarinfo = tarfile.TarInfo('README.txt')
        tarinfo.size = len(readme)
        tarinfo.uid = 0
        tarinfo.gid = 0
        tarinfo.uname = 'gemini'
        tarinfo.gname = 'gemini'
        tarinfo.mtime = time.time()
        tarinfo.mode = 0644
        # - and add it to the tar file
        buffer = cStringIO.StringIO(readme)
        tar.addfile(tarinfo, buffer)
        buffer.close()

        # All done
        tar.close()
        req.flush()

    finally:
        session.close()

    return apache.OK


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

    # Send them the data
    req.content_type = 'application/fits'
    req.headers_out['Content-Disposition'] = 'attachment; filename="%s"' % str(header.diskfile.file.name)
    if(using_s3):
        # S3 file server
        s3conn = S3Connection(aws_access_key, aws_secret_key)
        bucket = s3conn.get_bucket(s3_bucket_name)
        key = bucket.get_key(header.diskfile.filename)
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
            req.sendfile(header.diskfile.fullpath())
