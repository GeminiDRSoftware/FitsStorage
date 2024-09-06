import dateutil.parser
import json
import os
import stat
from datetime import datetime, timedelta
import hashlib

from sqlalchemy.exc import NoResultFound, MultipleResultsFound

from fits_storage.server.orm.miscfile import MiscFile, normalize_diskname
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.file import File

from fits_storage.web import templating

from fits_storage.gemini_metadata_utils import GeminiProgram

from fits_storage.server.access_control_utils import icanhave

from fits_storage.server.wsgi.context import get_context
from fits_storage.server.wsgi.returnobj import Return

from fits_storage.web.user import needs_login

from fits_storage.queues.queue.fileopsqueue import FileopsQueue, FileOpsRequest

from fits_storage.server.orm.fileuploadlog import FileUploadLog

from fits_storage.logger import DummyLogger

from fits_storage.config import get_config

SEARCH_LIMIT = 500


def miscfiles(handle=None):
    formdata = None
    try:
        formdata = get_context().get_form_data(large_file=True)
        if handle is None:
            if 'search' in formdata:
                return search_miscfiles(formdata)

            if 'upload' in formdata:
                return save_file(formdata)
        else:
            return detail_miscfile(handle, formdata)

        return bare_page()
    finally:
        if formdata and formdata.uploaded_file is not None:
            try:
                os.unlink(formdata.uploaded_file.name)
            except OSError:
                pass


@templating.templated("miscfiles/miscfiles.html")
def bare_page():
    user = get_context().user
    can_add = user.misc_upload if user else False
    return dict(can_add=can_add)


def enumerate_miscfiles(query):
    ctx = get_context()
    for misc, disk, file in query:
        yield icanhave(ctx, misc), misc, disk, file


@templating.templated("miscfiles/miscfiles.html")
def search_miscfiles(formdata):
    ctx = get_context()

    ret = dict(can_add=ctx.user.misc_upload)
    query = ctx.session.query(MiscFile, DiskFile, File).\
        join(DiskFile, MiscFile.diskfile_id == DiskFile.id).\
        join(File, DiskFile.file_id == File.id).\
        filter(DiskFile.canonical == True)

    message = []

    name = formdata['name'].value.strip() if 'name' in formdata else ''
    # Make sure there are no '&' in the keywords
    keyw = ' '.join(formdata['keyw'].value.split('&')).strip() \
        if 'keyw' in formdata else ''

    prog = formdata['prog'].value.strip() if 'prog' in formdata else ''

    if name:
        query = query.filter(File.name.like('%' + name + '%'))
        ret['searchName'] = name

    if keyw:
        query = query.\
            filter(MiscFile.description.match(' & '.join(keyw.split())))
        ret['searchKeyw'] = keyw

    if prog:
        query = query.filter(MiscFile.program_id.like('%' + prog + '%'))
        ret['searchProg'] = prog

    ret['uri'] = ctx.env.uri

    query = query.order_by(File.name).limit(SEARCH_LIMIT)
    cnt = query.count()
    ret['count'] = cnt
    ret['hit_limit'] = (cnt == SEARCH_LIMIT)
    ret['fileList'] = enumerate_miscfiles(query)

    return ret


def string_to_date(string):
    # May raise ValueError if the format is wrong or the date is invalid
    return datetime.strptime(string, '%Y-%m-%d')


def validate():
    ctx = get_context()

    raw_data = ctx.raw_data

    try:
        input_data = json.loads(raw_data)
        response = {'result': True}
        if 'release' in input_data:
            try:
                dateutil.parser.parse(input_data['release'])
            except ValueError:
                response['result'] = False
        if 'program' in input_data and response['result']:
            prog = GeminiProgram(input_data['program'])
            response['result'] = prog.valid
        if 'release' not in input_data and 'program' not in input_data:
            raise ValueError
    except ValueError:
        ctx.resp.status = Return.HTTP_BAD_REQUEST
        return

    ctx.resp.content_type = 'application/json'
    ctx.resp.append_json(response)


@needs_login(misc_upload=True)
@templating.templated("miscfiles/miscfiles.html")
def save_file(formdata):
    fsc = get_config()
    fileitem = formdata['uploadFile'].uploaded_file
    localfilename = normalize_diskname(fileitem.name)
    fullpath = os.path.join(fsc.upload_staging_dir, localfilename)
    jsonpath = fullpath + '.json'
    current_data = {}

    for item in ('uploadProg', 'uploadDesc'):
        try:
            current_data[item] = formdata[item]
        except KeyError:
            pass

    uploadRelease = formdata.getvalue('uploadRelease', '').strip()
    if uploadRelease == 'default':
        # Now + 18 pseudo-months
        release_date = datetime.now() + timedelta(days=540)
    elif uploadRelease == 'now':
        release_date = datetime.now()
    else:
        try:
            release_date = string_to_date(formdata.getvalue('arbRelease', '')
                                          .strip())
        except (ValueError, KeyError):
            return dict(can_add=True,
                        errorMessage="Wrong value for release date",
                        **current_data)

    ctx = get_context()
    # Add the initial fileuploadlog entry
    fileuploadlog = FileUploadLog(ctx.usagelog)
    fileuploadlog.filename = localfilename
    fileuploadlog.processed_cal = False
    ctx.session.add(fileuploadlog)

    # Content Length may or may not be defined. It's not required and if the
    # exporter is compressing on-the-fly, it won't know the length of the
    # compressed data ahead of time. Still, it's useful to log it.
    content_length = ctx.env['CONTENT_LENGTH']
    content_length = int(content_length) if content_length else None
    fileuploadlog.add_note(f"Content_Length header gave: {content_length}")
    ctx.session.commit()

    # Python's wsgiref.simple_server has a bug with .read() that causes the
    # read to hang in certain situations. We can work around this if
    # content-length is set, but not if not. This is what bytes_left does.
    # https://github.com/python/cpython/issues/66077

    # Stream the data into the upload_staging file.
    # Calculate the md5 and size as we do it
    m = hashlib.md5()
    size = 0
    chunksize = 1000000  # 1MB
    try:
        with open(fullpath, 'wb') as staging_file:
            fileuploadlog.ut_transfer_start = datetime.utcnow()
            read_file = formdata['uploadFile'].file
            read_file.seek(0)
            while chunk := read_file.read(chunksize):
                size += len(chunk)
                m.update(chunk)
                staging_file.write(chunk)
                # Work around simple_server bug if content_length is set.
                bytes_left = content_length - size if content_length else None
                if content_length and (bytes_left < chunksize):
                    chunksize = bytes_left

        fileuploadlog.ut_transfer_complete = datetime.utcnow()
        fileuploadlog.size = size
        fileuploadlog.md5 = m.hexdigest()
        ctx.session.commit()
        os.chmod(fullpath, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        with open(jsonpath, 'w') as meta:
            json.dump({'filename': fileitem.name,
                       'is_misc':  'True',
                       'release':  release_date.strftime('%Y-%m-%d'),
                       'description': formdata['uploadDesc'].value,
                       'program': formdata['uploadProg'].value},
                      meta)

        fq = FileopsQueue(ctx.session, logger=DummyLogger())

        fo_req = FileOpsRequest(request='ingest_upload',
                                args={'filename': localfilename,
                                      'fileuploadlog_id': fileuploadlog.id,
                                      'processed_cal': False})

        fq.add(fo_req, filename=localfilename, response_required=False)

    except IOError:
        # Clean up files
        for fn in (fullpath, jsonpath):
            if os.path.exists(fn):
                try:
                    os.unlink(fn)
                except IOError:
                    pass
        # TODO: We should log the failure
        return dict(can_add=True,
                    errorMessage="Error when trying to save the file",
                    **current_data)

    return dict(can_add=True)


@templating.templated("miscfiles/detail.html")
def detail_miscfile(handle, formdata={}):
    ctx = get_context()

    try:
        query = ctx.session.query(MiscFile, DiskFile, File)\
            .join(DiskFile, MiscFile.diskfile_id == DiskFile.id)\
            .join(File, DiskFile.file_id == File.id)
        try:
            meta, df, fobj = query.filter(MiscFile.id == int(handle)).one()
        except ValueError:
            # Must be a file name...
            meta, df, fobj = query.filter(File.name == handle).one()

        ret = dict(
            canedit=ctx.user.misc_upload,
            canhave=icanhave(ctx, meta),
            uri=ctx.env.uri,
            meta=meta,
            disk=df,
            file=fobj
            )

        if 'save' in formdata:
            release = formdata.getvalue('release', '').strip()
            if release == 'default':
                # Now + 18 pseudo-months
                release_date = datetime.now() + timedelta(days=540)
            elif release == 'now':
                release_date = datetime.now()
            else:
                try:
                    release_date = string_to_date(
                        formdata.getvalue('arbRelease', '').strip())
                except (ValueError, KeyError):
                    ret['errorMessage'] = "Wrong value for release date"
                    return ret

            meta.release = release_date
            meta.program_id = formdata.getvalue('prog', '')
            meta.description = formdata.getvalue('desc', '')
            ctx.session.flush()
            ret['message'] = "Successfully updated"

        return ret
    except NoResultFound:
        ctx.resp.client_error(Return.HTTP_NOT_FOUND,
                              "Could not find the required content")
    except MultipleResultsFound:
        ctx.resp.client_error(Return.HTTP_INTERNAL_SERVER_ERROR,
                              "More than one file was found matching the "
                              "provided name. This is an error!")
