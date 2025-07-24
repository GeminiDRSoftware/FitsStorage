import time
import datetime
import bz2
from io import BytesIO
import tarfile
import os

from sqlalchemy.exc import NoResultFound, MultipleResultsFound

from fits_storage.gemini_metadata_utils import gemini_fitsfilename

from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header

from fits_storage.server.orm.obslog import Obslog
from fits_storage.server.orm.downloadlog import DownloadLog
from fits_storage.server.orm.filedownloadlog import FileDownloadLog
from fits_storage.server.orm.miscfile import MiscFile

from fits_storage.server.wsgi.context import get_context
from fits_storage.server.wsgi.returnobj import Return

from fits_storage.server.access_control_utils import icanhave

from fits_storage.db.selection import Selection
from fits_storage.db.list_headers import list_headers

from fits_storage.config import get_config
fsc = get_config()

# Servers used as archive use a calibration association cache table
if fsc.is_archive:
    from fits_storage.cal.associate_calibrations import associate_cals_from_cache as associate_cals
else:
    from fits_storage.cal.associate_calibrations import associate_cals

if fsc.using_s3:
    from fits_storage.server.aws_s3 import Boto3Helper
    s3 = Boto3Helper()

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
Note that this download was from an associated calibrations page -
it only contains the calibration files that are associated with the science 
query.

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
    selection = Selection()
    selection['filelist'] = thelist
    return download(selection = selection,
                    associated_calibrations = False)


def download(selection, associated_calibrations):
    """
    This is the download server. Given a selection, it will send a tarball of the
    files from the selection that you have access to to the client.
    """
    fsc = get_config()

#    # First check if this is an associated_calibrations download
#    if 'associated_calibrations' in things:
#        associated_calibrations = True
#        things.remove('associated_calibrations')
#    # Get the selection
#    selection = getselection(things)

    selection['present'] = True

    ctx = get_context()
    ctx.resp.content_type = "application/tar"
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
        ctx.resp.append("No files to download. Either you asked to download "
                        "marked files, but didn't mark any files, or you "
                        "specified a selection criteria that doesn't find "
                        "any files")
        session.commit()
        return

    if selection.openquery and len(headers) > fsc.fits_open_result_limit:
        # Open query. Almost certainly too many files
        downloadlog.sending_files = False
        downloadlog.add_note("Hit Open result Limit, aborted")
        ctx.resp.content_type = 'text/plain'
        ctx.resp.append_iterable(
            ["Your selection criteria does not restrict the number of results,"
             " and more than %d were found. " % fsc.fits_open_result_limit,
             "Please refine your selection more before attempting to download. "
             "Queries that can contain an arbitrary number of results have a "
             "lower limit applied than more constrained queries. Including a "
             "date range or program id will prevent an arbitrary number of "
             "results being found will raise the limit"]
        )
        session.commit()
        return

    if len(headers) > fsc.fits_closed_result_limit:
        # Open query. Almost certainly too many files
        downloadlog.sending_files = False
        downloadlog.add_note("Hit Closed result limit, aborted")
        ctx.resp.content_type = 'text/plain'
        ctx.append_iterable(
            [f"More than {fsc.fits_closed_result_limit} results were found. "
             "This is beyond the limit we allow. Please refine your selection "
             "more before attempting to download. If you really want all "
             "these files, we suggest you break your search into several "
             "smaller date range pieces and download one set at a time."]
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
                # File has been updated behind our backs. Try to find the new
                # one
                file_id = header.diskfile.file_id
                header = session.query(Header).join(DiskFile)\
                    .filter(DiskFile.file_id == file_id)\
                    .filter(DiskFile.present == True).one()
                filedownloadlog.add_note("File replaced during download")
            filedownloadlog.diskfile_filename = header.diskfile.filename
            filedownloadlog.diskfile_file_md5 = header.diskfile.file_md5
            filedownloadlog.diskfile_file_size = header.diskfile.file_size
            session.add(filedownloadlog)
            path = header.diskfile.path
            filename = header.diskfile.filename
            md5 = header.diskfile.file_md5
            if icanhave(ctx, header, filedownloadlog):
                filedownloadlog.canhaveit = True

                md5file += f"{md5}  {path}/{filename}\n" if path else f"{md5}  {filename}\n"
                if fsc.using_s3:
                    keyname = f"{path}/{filename}" if path else filename
                    flo = s3.get_flo(keyname)
                    # Write buffer into tarfile
                    # - create a tarinfo object
                    tarinfo = make_tarinfo(
                        keyname,
                        size = header.diskfile.file_size,
                        uid = 0, gid = 0,
                        uname = 'gemini', gname = 'gemini',
                        mtime = time.mktime(header.diskfile.lastmod.timetuple()),
                        mode = 0o644
                    )
                    # - and add it to the tar file
                    try:
                        tar.addfile(tarinfo, flo)
                    except IOError:
                        downloadlog.add_note(f"IOError while adding {keyname} "
                                             f"to tarfile")
                        downloadlog.add_note(f"{flo.tell()=}, {flo.closed=}, "
                                             f"{tarinfo.size=}, "
                                             f"{header.diskfile.file_size=}")
                        session.commit()
                        raise

                else:
                    tar.add(header.diskfile.fullpath,
                            f"{path}/{filename}" if path else filename)
            else:
                # Permission denied, add to the denied list
                filedownloadlog.canhaveit = False
                denied.append(f"{path}/{filename}" if path else filename)
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
        readme = readme_body.format(selection_url=selection.to_url(),
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
        raise MultipleResultsFound("Multiple regular files found!")

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
        raise MultipleResultsFound("Multiple miscfiles found!")

supported_tests = (
    is_regular_file,
    is_obslog,
    is_misc
)

def fileserver(things):
    """
    This is the fileserver function. It always sends exactly one file.
    It handles authentication for serving the files too.
    The "filenamegiven" includes the path, allowing specific processing_tag
    versions to be downloaded. If the request is for the .bz2 version, we send
    that, if the request is for the .fits version, we send that.
    """

    ctx = get_context()
    session = ctx.session

    if len(things) == 0:
        return Return.HTTP_BAD_REQUEST
    elif len(things) == 1:
        filename = things[0]
        path = ''
    else:
        filename = things.pop(-1)
        path = '/'.join(things)

    # Instantiate the download log
    downloadlog = DownloadLog(ctx.usagelog)
    session.add(downloadlog)
    downloadlog.query_started = datetime.datetime.utcnow()

    # Form the basic query
    query = session.query(DiskFile).filter(DiskFile.present == True)

    # If no path was provided, it will be an empty string, and that is also
    # what the diskfile tables has for files in the "root" of the path, so we
    # don't need a conditional on this
    query = query.filter(DiskFile.path == path)

    # Need to handle the presence or lack of .bz2.
    # Record which was requested for later
    filenamerequested = filename
    try:
        # First just query as is.
        diskfile = query.filter(DiskFile.filename == filename).one()
    except MultipleResultsFound:
        downloadlog.add_note("Error! Multiple present files found!")
        ctx.resp.status = Return.HTTP_BAD_REQUEST
        return
    except NoResultFound:
        # "Flip" the .bz2 of the filename
        if filename.endswith('.bz2'):
            filename = filename[:-4]
        else:
            filename += '.bz2'

        # and search again
        try:
            diskfile = query.filter(DiskFile.filename == filename).one()
        except MultipleResultsFound:
            downloadlog.add_note("Error! Multiple present files found!")
            ctx.resp.status = Return.HTTP_BAD_REQUEST
            return
        except NoResultFound:
            diskfile = None


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
        filedownloadlog = FileDownloadLog(ctx.usagelog)
        filedownloadlog.diskfile_filename = diskfile.filename
        filedownloadlog.diskfile_file_md5 = diskfile.file_md5
        filedownloadlog.diskfile_file_size = diskfile.file_size
        session.add(filedownloadlog)
        # Is the client allowed to get this file?
        if icanhave(ctx, item, filedownloadlog):
            filedownloadlog.canhaveit = True
            # Send them the data
            downloadlog.sending_files = True
            sendonefile(item.diskfile, content_type=content_type,
                        filenamegiven=filenamerequested)
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
        self.unused_bytes = b''

    def __iter__(self):
        return self

    def __next__(self):
        data = self.read(4096)
        if data is None or len(data) == 0:
            raise StopIteration
        return data

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


class BZ2OnTheFlyCompressor(object):
    def __init__(self, buff):
        self.buff = buff
        self.comp = bz2.BZ2Compressor()
        self.unused_bytes = b''
        self.done = False

    def __iter__(self):
        return self

    def __next__(self):
        data = self.read(4096)
        if data is None or len(data) == 0:
            raise StopIteration
        return data
    
    def _buffer(self, limit):
        while not self.done and len(self.unused_bytes) < limit:
            chunk = self.buff.read(CHUNKSIZE)
            if not chunk:
                comp = self.comp.flush()
                if comp:
                    self.unused_bytes += comp
                self.done = True
            else:
                comp = self.comp.compress(chunk)
                if comp:
                    self.unused_bytes += comp

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


def sendonefile(diskfile, content_type=None, filenamegiven=None):
    """
    Send the (one) fits file referred to by the diskfile object to the
    client. This sends data as compressed or uncompressed depending on the
    given filename extension (.bz2 for compressed).  If no given filename is
    passed, the filename is taken from the diskfile entry.
    """
    fsc = get_config()
    ctx = get_context()
    resp = ctx.resp

    # Send them the data
    if content_type is not None:
        resp.content_type = content_type

    fname = diskfile.filename
    path = diskfile.path
    if filenamegiven is None:
        filenamegiven = fname

    if content_type == 'application/fits':
        resp.set_header('Content-Disposition', 'attachment; filename="%s"' % filenamegiven)

    if fsc.using_s3:
        # S3 file server
        keyname = f"{diskfile.path}/{diskfile.filename}" if diskfile.path else \
            diskfile.filename
        flo = s3.get_flo(keyname)
        if diskfile.compressed:
            if filenamegiven.lower().endswith('.bz2'):
                resp.content_length = diskfile.file_size
                resp.sendfile_obj(flo)
            else:
                resp.content_length = diskfile.data_size
                resp.sendfile_obj(BZ2OnTheFlyDecompressor(flo))
        else:
            if filenamegiven.lower().endswith('.bz2'):
                resp.sendfile_obj(BZ2OnTheFlyCompressor(flo))
            else:
                resp.content_length = diskfile.file_size
                resp.sendfile_obj(flo)
    else:
        # Serve from regular file
        try:
            if diskfile.compressed == True:
                if filenamegiven.lower().endswith('.bz2'):
                    resp.content_length = diskfile.file_size
                    resp.sendfile(diskfile.fullpath)
                else:
                    # Unzip it on the fly
                    resp.content_length = diskfile.data_size
                    resp.sendfile_obj(BZ2OnTheFlyDecompressor(open(diskfile.fullpath, 'rb')))
            else:
                if filenamegiven.lower().endswith('.bz2'):
                    resp.sendfile_obj(BZ2OnTheFlyCompressor(open(diskfile.fullpath, 'rb')))
                else:
                    resp.sendfile(diskfile.fullpath)
        except IOError:
            ctx.resp.client_error(Return.HTTP_NOT_FOUND, unexpected_not_found_template.format(fname=filenamegiven))
