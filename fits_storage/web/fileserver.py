from ..orm import NoResultFound, MultipleResultsFound

from ..fits_storage_config import using_s3, fits_open_result_limit, fits_closed_result_limit

from ..gemini_metadata_utils import gemini_fitsfilename
from ..orm.file import File
from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.obslog import Obslog
from ..orm.downloadlog import DownloadLog
from ..orm.filedownloadlog import FileDownloadLog
from ..orm.miscfile import MiscFile

from ..utils.web import get_context, Return, with_content_type

from .selection import getselection, openquery, selection_to_URL
from .summary import list_headers

import time
import datetime
import bz2
from io import StringIO, BytesIO
import tarfile
import os

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

def make_tarinfo(name, **kw):
    ti = tarfile.TarInfo(name)
    for key, value in list(kw.items()):
        setattr(ti, key, value)
    return ti

def download_post():
    # Parse form data
    formdata = get_context().req.get_form_data()
    thelist = []
    if 'files' in formdata: #hasattr(formdata, 'files'):
        fields = formdata["files"]
        if isinstance(fields, list):
            for field in fields:
                thelist.append(str(field.value))
        else:
            thelist.append(fields.value)
    return download(selection = {'filelist': thelist},
                    associated_calibrations = False)

@with_content_type("application/tar")
def download(selection, associated_calibrations):
    """
    This is the download server. Given a selection, it will send a tarball of the
    files from the selection that you have access to to the client.
    """

#    # First check if this is an associated_calibrations download
#    if 'associated_calibrations' in things:
#        associated_calibrations = True
#        things.remove('associated_calibrations')
#    # Get the selection
#    selection = getselection(things)

    selection['present'] = True

    ctx = get_context()
    # Open a database session

    session = ctx.session

    # Instantiate the download log
    downloadlog = DownloadLog(ctx.usagelog)
    session.add(downloadlog)
    downloadlog.selection = str(selection)
    downloadlog.query_started = datetime.datetime.utcnow()

    # Get our username while we have the database session open
    user = ctx.user
    if user:
        username = user.username
    else:
        username = 'Not Logged In'

    # Get the header list
    headers = list_headers(selection, None)
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
        ctx.resp.content_type = 'text/plain'
        ctx.resp.append("No files to download. Either you asked to download marked files, but didn't mark any files, or you specified a selection criteria that doesn't find any files")
        session.commit()
        return

    if openquery(selection) and len(headers) > fits_open_result_limit:
        # Open query. Almost certainly too many files
        downloadlog.sending_files = False
        downloadlog.add_note("Hit Open result Limit, aborted")
        ctx.resp.content_type = 'text/plain'
        ctx.resp.append_iterable(
            ["Your selection criteria does not restrict the number of results, and more than %d were found. " % fits_open_result_limit,
             "Please refine your selection more before attempting to download. Queries that can contain an arbitrary number of results have a lower limit applied than more constrained queries. Including a date range or program id will prevent an arbitrary number of results being found will raise the limit"]
        )
        session.commit()
        return

    if len(headers) > fits_closed_result_limit:
        # Open query. Almost certainly too many files
        downloadlog.sending_files = False
        downloadlog.add_note("Hit Closed result limit, aborted")
        ctx.resp.content_type = 'text/plain'
        ctx.append_iterable(
            ["More than %d results were found. This is beyond the limit we allow" % fits_closed_result_limit,
             "Please refine your selection more before attempting to download. If you really want all these files, we suggest you break your search into several smaller date range pieces and download one set at a time."]
        )
        session.commit()
        return

    # Set up the http headers
    downloadlog.sending_files = True
    tarfilename = generate_filename(associated_calibrations, selection)

    # We are going to build an md5sum file while we do this
    md5file = ""
    # And keep a list of any files we were denied
    denied = []
    # Here goes!
    with ctx.resp.tarfile(tarfilename, mode="w|") as tar:
        for header in headers:
            filedownloadlog = FileDownloadLog(ctx.usagelog)
            if not header.diskfile.present:
                # File has been updated behind our backs. Try to find the new one
                file_id = header.diskfile.file_id
                header = session.query(Header).join(DiskFile).filter(DiskFile.file_id == file_id).filter(DiskFile.present == True).one()
                filedownloadlog.add_note("File replaced during download")
            filedownloadlog.diskfile_filename = header.diskfile.filename
            filedownloadlog.diskfile_file_md5 = header.diskfile.file_md5
            filedownloadlog.diskfile_file_size = header.diskfile.file_size
            session.add(filedownloadlog)
            if icanhave(ctx, header, filedownloadlog):
                filedownloadlog.canhaveit = True
                md5file += "%s  %s\n" % (header.diskfile.file_md5, header.diskfile.filename)
                if using_s3:
                    with s3.fetch_temporary(header.diskfile.filename) as buffer:
                        # Write buffer into tarfile
                        # - create a tarinfo object
                        tarinfo = make_tarinfo(
                            header.diskfile.filename,
                            size = header.diskfile.file_size,
                            uid = 0, gid = 0,
                            uname = 'gemini', gname = 'gemini',
                            mtime = time.mktime(header.diskfile.lastmod.timetuple()),
                            mode = 0o644
                        )
                        # - and add it to the tar file
                        try:
                            tar.addfile(tarinfo, buffer)
                        except IOError:
                            downloadlog.add_note("IOError while adding %s to tarfile" % header.diskfile.filename)
                            downloadlog.add_note("buffer filename: %s tell: %s closed: %s" % (buffer.name, buffer.tell(), buffer.closed))
                            downloadlog.add_note("ti size: %s, df size: %s" % (tarinfo.size, header.diskfile.file_size))
                            st = os.stat(buffer.name)
                            downloadlog.add_note("st_size: %s" % st.st_size)
                            session.commit()
                            raise

                else:
                    tar.add(header.diskfile.fullpath(), header.diskfile.filename)
            else:
                # Permission denied, add to the denied list
                filedownloadlog.canhaveit = False
                denied.append(header.diskfile.filename)
        downloadlog.numdenied = len(denied)
        # OK, that's all the fits files. Add the md5sum file
        # - create a tarinfo object
        tarinfo = make_tarinfo(
            'md5sums.txt',
            size = len(md5file),
            uid = 0, gid = 0,
            uname = 'gemini', gname = 'gemini',
            mtime = time.time(),
            mode = 0o644
        )
        # - and add it to the tar file
        tar.addfile(tarinfo, BytesIO(md5file.encode('utf8')))

        # And add the README.TXT file
        readme = readme_body.format(selection_url=selection_to_URL(selection),
                                    search_time=datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                                    username=username)
        if associated_calibrations:
            readme += readme_associated
        if denied:
            readme += readme_denied.format(denied = '\n'.join(denied))
        # - create a tarinfo object
        tarinfo = make_tarinfo(
            'README.txt',
            size = len(readme),
            uid = 0, gid = 0,
            uname = 'gemini', gname = 'gemini',
            mtime = time.time(),
            mode = 0o644
        )
        # - and add it to the tar file
        tar.addfile(tarinfo, BytesIO(readme.encode('utf8')))

    downloadlog.download_completed = datetime.datetime.utcnow()

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

def fileserver(filenamegiven):
    """
    This is the fileserver funciton. It always sends exactly one fits file, uncompressed.
    It handles authentication for serving the files too
    """

    ctx = get_context()

#    # OK, first find the file they asked for in the database
#    # tart up the filename if possible
#    if not things:
#        ctx.resp.status = Return.HTTP_NOT_FOUND
#        return

#    filenamegiven = things.pop(0)
    filename = gemini_fitsfilename(filenamegiven)
    if not filename:
        filename = filenamegiven

    session = ctx.session

    # Instantiate the download log
    downloadlog = DownloadLog(ctx.usagelog)
    session.add(downloadlog)
    downloadlog.query_started = datetime.datetime.utcnow()

    try:
        file = session.query(File).filter(File.name == filename).one()
        # OK, we should have the file record now.
        # Next, find the canonical diskfile for it
        diskfile = (
            session.query(DiskFile)
                    .filter(DiskFile.present == True)
                    .filter(DiskFile.file_id == file.id)
                    .one()
            )
        # Note that either of those queries can trigger NoResultFound
    except NoResultFound:
        downloadlog.add_note("Not found in File table")
        ctx.resp.status = Return.HTTP_NOT_FOUND
        return


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
        if icanhave(ctx, item):
            # Send them the data
            downloadlog.sending_files = True
            sendonefile(item.diskfile, content_type=content_type)
            downloadlog.download_completed = datetime.datetime.utcnow()
        else:
            # Refuse to send data
            downloadlog.numdenied = 1
            ctx.resp.client_error(Return.HTTP_FORBIDDEN, "Not enough privileges to download this content")

CHUNKSIZE = 8192*64

class BZ2OnTheFlyDecompressor(object):
    def __init__(self, buff):
        self.buff = buff
        self.decomp = bz2.BZ2Decompressor()
        self.unused_bytes = ''

    def _buffer(self, limit):
        while len(self.unused_bytes) < limit:
            chunk = self.buff.read(CHUNKSIZE)
            if not chunk:
                break
            decomp = self.decomp.decompress(chunk)
            if decomp:
                self.unused_bytes += decomp

    def read(self, k):
        if len(self.unused_bytes) < k:
            self._buffer(k)

        if not self.unused_bytes:
            return None

        ret = self.unused_bytes[:k]
        self.unused_bytes = self.unused_bytes[k:]

        return ret

unexpected_not_found_template = """
The file '{fname}' could not be found in the system.</p>
This was unexpected. Please, inform the administrators.
"""

def sendonefile(diskfile, content_type=None):
    """
    Send the (one) fits file referred to by the diskfile object to the client.
    This always sends unzipped data.
    """

    ctx = get_context()
    resp = ctx.resp

    # Send them the data
    if content_type is not None:
        resp.content_type = content_type

    if content_type == 'application/fits':
        resp.set_header('Content-Disposition', 'attachment; filename="%s"' % str(diskfile.file.name))


    fname = diskfile.filename
    if using_s3:
        # S3 file server
        resp.content_length = diskfile.data_size
        with s3.fetch_temporary(fname) as buffer:
            if diskfile.compressed:
                resp.sendfile_obj(BZ2OnTheFlyDecompressor(buffer))
            else:
                resp.sendfile_obj(buffer)
    else:
        # Serve from regular file
        try:
            if diskfile.compressed == True:
                # Unzip it on the fly
                resp.content_length = diskfile.data_size
                resp.sendfile_obj(BZ2OnTheFlyDecompressor(open(diskfile.fullpath(), 'r')))
            else:
                resp.sendfile(diskfile.fullpath())
        except IOError:
            ctx.resp.client_error(Return.HTTP_NOT_FOUND, unexpected_not_found_template.format(fname = fname))
