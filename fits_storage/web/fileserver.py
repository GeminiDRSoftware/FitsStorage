from ..orm import session_scope, NoResultFound, MultipleResultsFound

from ..fits_storage_config import using_s3, fits_open_result_limit, fits_closed_result_limit

from ..gemini_metadata_utils import gemini_fitsfilename
from ..orm.file import File
from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.obslog import Obslog
from ..orm.downloadlog import DownloadLog
from ..orm.filedownloadlog import FileDownloadLog
from ..orm.miscfile import MiscFile

from .selection import getselection, openquery, selection_to_URL
from .summary import list_headers
from .user import userfromcookie, AccessForbidden, DEFAULT_403_TEMPLATE

# This will only work with apache
from mod_python import apache
from mod_python import util

import time
import datetime
import bz2
import cStringIO
import tarfile

# We assume that servers used as archive use a calibraiton association cache table
from ..fits_storage_config import use_as_archive
if use_as_archive:
    from ..cal.associate_calibrations import associate_cals_from_cache as associate_cals
else:
    from ..cal.associate_calibrations import associate_cals

if using_s3:
    from ..utils.aws_s3 import get_helper
    s3 = get_helper()

from ..utils.userprogram import icanhave

filename_elements = (
    'program_id',
    'observation_id',
    'inst',
    'date',
    'daterange',
    'obsclass',
    'obstype'
    )

def generate_filename(cals, selection):
    content_type = 'data' if not cals else 'calibs'
    name_components = []
    for element in filename_elements:
        if not selection.get(element):
            continue
        name_components.append(selection[element])

    if name_components:
        return 'gemini_{type}.{sel}.tar'.format(type=content_type,
                                                sel='_'.join(name_components))
    else:
        return 'gemini_{type}.tar'.format(type=content_type)

readme_body = """\
This is a tar file of search results downloaded from the gemini archive.

The search criteria was: {selection_url}
The search was performed at: {search_time} UTC
The search was performed by archive user: {username}

We have included a file listing the md5sums of the data files in here.
If you have the 'md5sum' utility installed (most Linux machines at least),
You can verify file integrity by running 'md5sum -c md5sums.txt'.

"""

readme_associated = """\
Note that this download was from an assoicated calibrations page -
it only contains the calibration files that are associated with the science query.

"""

readme_denied = """\
The following files in your search results were not included,
because they are proprietary data that you do not have access to:
{denied}
"""

def download(req, things):
    """
    This is the download server. Given a selection, it will send a tarball of the
    files from the selection that you have access to to the client.
    """
    # assume unless set otherwise later that this is not an associated_calibrations download
    associated_calibrations = False
    # If we are called via POST, then parse form data rather than selection
    if req.method == 'POST':
        # Parse form data
        formdata = util.FieldStorage(req)
        thelist = []
        if 'files' in formdata.keys():
            fields = formdata['files']
            if isinstance(fields, list):
                for field in fields:
                    thelist.append(str(field.value))
            else:
                thelist.append(str(fields))
        selection = {'filelist': thelist}
        selection['present'] = True
    else:
        # First check if this is an associated_calibrations download
        if 'associated_calibrations' in things:
            associated_calibrations = True
            things.remove('associated_calibrations')
        # Get the selection
        selection = getselection(things)

    # Open a database session
    with session_scope() as session:
        # Instantiate the download log
        downloadlog = DownloadLog(req.usagelog)
        session.add(downloadlog)
        downloadlog.selection = str(selection)
        downloadlog.query_started = datetime.datetime.utcnow()

        # Get our username while we have the database session open
        user = userfromcookie(session, req)
        if user:
            username = user.username
        else:
            username = 'Not Logged In'

        # Get the header list
        headers = list_headers(session, selection, None)
        # If this is an associated_calibrations request, do that now
        if associated_calibrations:
            downloadlog.add_note("associated_calibrations download")
            headers = associate_cals(session, headers)
        downloadlog.query_completed = datetime.datetime.utcnow()
        downloadlog.numresults = len(headers)

        if not headers:
            # No results. No point making a tar file
            downloadlog.sending_files = False
            downloadlog.add_note("Nothing to download, aborted")
            req.content_type = "text/plain"
            req.write("No files to download. Either you asked to download marked files, but didn't mark any files, or you specified a selection criteria that doesn't find any files")
            session.commit()
            return apache.HTTP_OK

        if openquery(selection) and len(headers) > fits_open_result_limit:
            # Open query. Almost certainly too many files
            downloadlog.sending_files = False
            downloadlog.add_note("Hit Open result Limit, aborted")
            req.content_type = "text/plain"
            req.write("Your selection criteria does not restrict the number of results, and more than %d were found. " %
                        fits_open_result_limit)
            req.write("Please refine your selection more before attempting to download. Queries that can contain an arbitrary number of results have a lower limit applied than more constrained queries. Including a date range or program id will prevent an arbitrary number of results being found will raise the limit")
            session.commit()
            return apache.HTTP_OK

        if len(headers) > fits_closed_result_limit:
            # Open query. Almost certainly too many files
            downloadlog.sending_files = False
            downloadlog.add_note("Hit Closed result limit, aborted")
            req.content_type = "text/plain"
            req.write("More than %d results were found. This is beyond the limit we allow" % fits_closed_result_limit)
            req.write("Please refine your selection more before attempting to download. If you really want all these files, we suggest you break your search into several smaller date range pieces and download one set at a time.")
            session.commit()
            return apache.HTTP_OK

        # Set up the http headers
        downloadlog.sending_files = True
        tarfilename = generate_filename(associated_calibrations, selection)
        req.content_type = "application/tar"
        req.headers_out['Content-Disposition'] = 'attachment; filename="{}"'.format(tarfilename)

        # We are going to build an md5sum file while we do this
        md5file = ""
        # And keep a list of any files we were denied
        denied = []
        # Here goes!
        tar = tarfile.open(name=tarfilename, mode="w|", fileobj=req)
        for header in headers:
            filedownloadlog = FileDownloadLog(req.usagelog)
            filedownloadlog.diskfile_filename = header.diskfile.filename
            filedownloadlog.diskfile_file_md5 = header.diskfile.file_md5
            filedownloadlog.diskfile_file_size = header.diskfile.file_size
            session.add(filedownloadlog)
            if icanhave(session, req, header, filedownloadlog):
                filedownloadlog.canhaveit = True
                md5file += "%s  %s\n" % (header.diskfile.file_md5, header.diskfile.filename)
                if using_s3:
                    with s3.fetch_temporary(header.diskfile.filename) as buffer:
                        # Write buffer into tarfile
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
                else:
                    tar.add(header.diskfile.fullpath(), header.diskfile.filename)
            else:
                # Permission denied, add to the denied list
                filedownloadlog.canhaveit = False
                denied.append(header.diskfile.filename)
        downloadlog.numdenied = len(denied)
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
        readme = readme_body.format(selection_url=selection_to_URL(selection),
                                    search_time=datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                                    username=username)
        if associated_calibrations:
            readme += readme_associated
        if denied:
            readme += readme_denied.format(denied = '\n'.join(denied))
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
        downloadlog.download_completed = datetime.datetime.utcnow()

    return apache.HTTP_OK

def is_regular_file(session, diskfile):
    try:
        header = session.query(Header).filter(Header.diskfile_id == diskfile.id).one()
        return header, 'application/fits'
    except MultipleResultsFound:
        raise MultipleResultsFound("Multiple files found!")

def is_obslog(session, diskfile):
    try:
        obslog = session.query(Obslog).filter(Obslog.diskfile_id == diskfile.id).one()
        return obslog, 'text/plain'
    except MultipleResultsFound:
        raise MultipleResultsFound("Multiple obslogs found!")

def is_misc(session, diskfile):
    try:
        miscfile = session.query(MiscFile).filter(MiscFile.diskfile_id == diskfile.id).one()
        return miscfile, 'application/octect-stream'
    except MultipleResultsFound:
        raise MultipleResultsFound("Multiple files found!")

supported_tests = (
    is_regular_file,
    is_obslog,
    is_misc
)

def fileserver(req, things):
    """
    This is the fileserver funciton. It always sends exactly one fits file, uncompressed.
    It handles authentication for serving the files too
    """

    # OK, first find the file they asked for in the database
    # tart up the filename if possible
    if not things:
        return apache.HTTP_NOT_FOUND

    filenamegiven = things.pop(0)
    filename = gemini_fitsfilename(filenamegiven)
    if not filename:
        filename = filenamegiven

    with session_scope(no_rollback=True) as session:
        # Instantiate the download log
        downloadlog = DownloadLog(req.usagelog)
        session.add(downloadlog)
        downloadlog.query_started = datetime.datetime.utcnow()

        try:
            file = session.query(File).filter(File.name == filename).one()
        except NoResultFound:
            downloadlog.add_note("Not found in File table")
            return apache.HTTP_NOT_FOUND
        # OK, we should have the file record now.
        # Next, find the canonical diskfile for it
        diskfile = (
            session.query(DiskFile)
                    .filter(DiskFile.present == True)
                    .filter(DiskFile.file_id == file.id)
                    .one()
            )

        item = None
        # And now find the header record...
        for is_file_type in supported_tests:
            try:
                item, content_type = is_file_type(session, diskfile)
                break
            except NoResultFound:
                # Not the kind of the object we were looking for
                pass
            except MultipleResultsFound as e:
                downloadlog.add_note(str(e))
                break

        downloadlog.query_completed = datetime.datetime.utcnow()
        downloadlog.numresults = 1
        if item is None:
            downloadlog.numresults = 0
        else:
            # Is the client allowed to get this file?
            if icanhave(session, req, item):
                # Send them the data
                downloadlog.sending_files = True
                sendonefile(req, item.diskfile, content_type=content_type)
                downloadlog.download_completed = datetime.datetime.utcnow()
            else:
                # Refuse to send data
                downloadlog.numdenied = 1
                raise AccessForbidden("Not enough privileges to download this content", DEFAULT_403_TEMPLATE)

        return apache.HTTP_OK

def sendonefile(req, diskfile, content_type=None):
    """
    Send the (one) fits file referred to by the diskfile object to the client
    referred to by the req obect. This always sends unzipped data.
    """

    # Send them the data
    if content_type is not None:
        req.content_type = content_type

    if content_type == 'application/fits':
        req.headers_out['Content-Disposition'] = 'attachment; filename="%s"' % str(diskfile.file.name)

    if using_s3:
        # S3 file server
        fname = diskfile.filename
        req.set_content_length(diskfile.data_size)
        with s3.fetch_temporary(fname) as buffer:
            data = buffer.read()
            if diskfile.compressed:
                req.write(bz2.decompress(data))
            else:
                req.write(data)
    else:
        # Serve from regular file
        if diskfile.compressed == True:
            # Unzip it on the fly
            req.set_content_length(diskfile.data_size)
            zfp = bz2.BZ2File(diskfile.fullpath(), 'r')
            try:
                req.write(zfp.read())
            finally:
                zfp.close()
        else:
            req.sendfile(diskfile.fullpath())
