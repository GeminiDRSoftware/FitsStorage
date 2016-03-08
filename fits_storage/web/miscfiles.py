from ..orm.miscfile import MiscFile, normalize_diskname
from ..orm.diskfile import DiskFile
from ..orm.file     import File
from ..orm          import NoResultFound, MultipleResultsFound

from . import templating

from ..fits_storage_config import upload_staging_path, api_backend_location

from ..gemini_metadata_utils import GeminiProgram

from ..utils.api import ApiProxy, ApiProxyError
from ..utils.userprogram import icanhave
from ..utils.web import Context

from .user import needs_login

from mod_python import util, apache
import dateutil
import json
import os
import stat
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

SEARCH_LIMIT = 500

class LargeFileFieldStorage(util.FieldStorage):
    def __init__(self, *args, **kw):
        self.uploaded_file = None

        kw['file_callback'] = self.file_callback
        util.FieldStorage.__init__(self, *args, **kw)

    def file_callback(self, name):
        fobj = NamedTemporaryFile(mode='w+b', suffix='.' + name, dir=upload_staging_path, delete=False)
        self.uploaded_file = fobj
        return fobj

def getFormData(req):
    return LargeFileFieldStorage(req)

def miscfiles(req, things):
    formdata = None
    try:
        if len(things) == 1 and things[0] == 'validate_add':
            return validate(req)

        formdata = getFormData(req)
        if len(things) == 0:
            if 'search' in formdata:
                return search_miscfiles(req, formdata)

            if 'upload' in formdata:
                return save_file(req, formdata)
        elif len(things) == 1:
            return detail_miscfile(req, things[0], formdata)

        return bare_page(req)
    finally:
        if formdata and formdata.uploaded_file is not None:
            try:
                os.unlink(formdata.uploaded_file.name)
            except OSError:
                pass

@templating.templated("miscfiles/miscfiles.html")
def bare_page(req):
    return dict(can_add=Context().is_staffer)

def enumerate_miscfiles(req, query):
    session = Context().session
    for misc, disk, file in query:
        yield icanhave(session, req, misc), misc, disk, file

@templating.templated("miscfiles/miscfiles.html")
def search_miscfiles(req, formdata):
    ctx = Context()

    ret = dict(can_add=ctx.is_staffer)
    query = ctx.session.query(MiscFile, DiskFile, File).join(DiskFile).join(File)

    message = []

    name = formdata.get('name', '').strip()
    # Make sure there are no '&' in the keywords
    keyw = ' '.join(formdata.get('keyw', '').split('&')).strip()
    prog = formdata.get('prog', '').strip()

    if name:
        query = query.filter(File.name.like('%' + name + '%'))
        ret['searchName'] = name

    if keyw:
        query = query.filter(MiscFile.description.match(' & '.join(keyw.split())))
        ret['searchKeyw'] = keyw

    if prog:
        query = query.filter(MiscFile.program_id.like('%' + prog + '%'))
        ret['searchProg'] = prog

    ret['uri'] = ctx.req.env.uri

    query = query.order_by(File.name).limit(SEARCH_LIMIT)
    cnt = query.count()
    ret['count'] = cnt
    ret['hit_limit'] = (cnt == SEARCH_LIMIT)
    ret['fileList'] = enumerate_miscfiles(req, query)

    return ret

def string_to_date(string):
    # May raise ValueError if the format is wrong or the date is invalid
    return datetime.strptime(string, '%Y-%m-%d')

def validate(req):
    ctx = Context()

    if req.method != 'POST':
        return apache.HTTP_NOT_ACCEPTABLE

    raw_data = ctx.req.raw_data

    try:
        input_data = json.loads(raw_data)
        response = {'result': True}
        if 'release' in input_data:
            try:
                dateutil.parser.parse(input_data['release'])
            except ValueError:
                response['result'] = False
        elif 'program' in input_data:
            prog = GeminiProgram(input_data['program'])
            response['result'] = prog.valid
        else:
            raise ValueError
    except ValueError:
        return apache.HTTP_BAD_REQUEST


    ctx.resp.content_type = 'application/json'
    ctx.resp.append_json(response)

    return apache.HTTP_OK

@needs_login(superuser=True)
@templating.templated("miscfiles/miscfiles.html")
def save_file(req, formdata):
    fileitem = formdata['uploadFile']
    localfilename = normalize_diskname(fileitem.filename)
    fullpath = os.path.join(upload_staging_path, localfilename)
    jsonpath = fullpath + '.json'
    current_data = {}

    def cleanup():
        for fn in (fullpath, jsonpath):
            if os.path.exists(fn):
                try:
                    os.unlink(fn)
                except IOError:
                    pass

    for item in ('uploadProg', 'uploadDesc'):
        try:
            current_data[item] = formdata[item]
        except KeyError:
            pass

    if formdata['uploadRelease'] == 'default':
        release_date = datetime.now() + timedelta(days=540) # Now + 18 pseudo-months
    elif formdata['uploadRelease'] == 'now':
        release_date = datetime.now()
    else:
        try:
            release_date = string_to_date(formdata['arbRelease'])
        except (ValueError, KeyError):
            return dict(can_add=True, errorMessage = "Wrong value for release date",
                        **current_data)

    try:
        os.rename(formdata.uploaded_file.name, fullpath)
        os.chmod(fullpath, stat.S_IRUSR|stat.S_IRGRP|stat.S_IROTH)
        with open(jsonpath, 'w') as meta:
            json.dump({'filename': fileitem.filename,
                       'is_misc':  'True',
                       'release':  release_date.strftime('%Y-%m-%d'),
                       'description': formdata.get('uploadDesc', None),
                       'program': formdata.get('uploadProg', None)},
                     meta)
        proxy = ApiProxy(api_backend_location)
        result = proxy.ingest_upload(filename=localfilename)
        return dict(can_add=True, actionMessage = "Ingested with result: " + str(result))
    except IOError:
        cleanup()
        # TODO: We should log the failure
        return dict(can_add=True, errorMessage = "Error when trying to save the file",
                    **current_data)
    except ApiProxyError:
        cleanup()
        return dict(can_add=True, errorMessage = "Error when trying to queue the file for ingestion",
                    **current_data)

    return dict(can_add=True)

@templating.templated("miscfiles/detail.html")
def detail_miscfile(req, handle, formdata):
    ctx = Context()

    try:
        query = ctx.session.query(MiscFile, DiskFile, File).join(DiskFile).join(File)
        try:
            meta, df, fobj = query.filter(MiscFile.id == int(handle)).one()
        except ValueError:
            # Must be a file name...
            meta, df, fobj = query.filter(File.name == handle).one()

        ret = dict(
            canedit = ctx.is_staffer,
            canhave = icanhave(ctx.session, req, meta),
            uri  = ctx.req.env.uri,
            meta = meta,
            disk = df,
            file = fobj
            )

        if 'save' in formdata:
            if formdata['release'] == 'default':
                release_date = datetime.now() + timedelta(days=540) # Now + 18 pseudo-months
            elif formdata['release'] == 'now':
                release_date = datetime.now()
            else:
                try:
                    release_date = string_to_date(formdata['arbRelease'])
                except (ValueError, KeyError):
                    ret['errorMessage'] = "Wrong value for release date"
                    return ret

            meta.release = release_date
            meta.program_id = formdata.get('prog', '')
            meta.description = formdata.get('desc', '')
            session.flush()
            ret['message'] = "Successfully updated"

        return ret
    except NoResultFound:
        return dict(error = "Can't find the required content")
    except MultipleResultsFound:
        return dict(error = "More than one file was found matching the provided name. This is an error!")
