from ..orm.miscfile import MiscFile, normalize_diskname
from ..orm.diskfile import DiskFile
from ..orm.file     import File
from ..orm          import NoResultFound, MultipleResultsFound

from . import templating

from ..fits_storage_config import upload_staging_path, api_backend_location

from ..utils.api import ApiProxy, ApiProxyError
from ..utils.userprogram import icanhave
from .user import needs_login, is_staffer

from mod_python import util
import json
import os
import stat
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

class LargeFileFieldStorage(util.FieldStorage):
    def __init__(self, *args, **kw):
        self.uploaded_file = None

        kw['file_callback'] = self.file_callback
        util.FieldStorage.__init__(self, *args, **kw)

    def file_callback(self, name):
        fobj = NamedTemporaryFile(mode='w+b', suffix='.' + name, dir=upload_staging_path, delete=False)
        self.uploaded_file = fobj
        return fobj

def miscfiles(req, things):
    try:
        formdata = LargeFileFieldStorage(req)

        if 'search' in formdata:
            return search_miscfiles(req, formdata)

        # TODO: Only Gemini staff should have access to this
        if 'upload' in formdata:
            return save_file(req, formdata)

        if len(things) == 1:
            return detail_miscfile(req, things[0])

        return bare_page(req)
    finally:
        if formdata.uploaded_file is not None:
            try:
                os.unlink(formdata.uploaded_file.name)
            except OSError:
                pass

@templating.templated("miscfiles/miscfiles.html")
def bare_page(req):
    return dict(can_add=is_staffer(req))

def enumerate_miscfiles(session, req, query):
    for misc, disk, file in query:
        yield icanhave(session, req, misc), misc, disk, file

@templating.templated("miscfiles/miscfiles.html", with_session=True)
def search_miscfiles(session, req, formdata):
    ret = dict(can_add=is_staffer(req, session))
    query = session.query(MiscFile, DiskFile, File).join(DiskFile).join(File)

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

    ret['uri'] = req.uri

    query = query.order_by(File.name).limit(500)
    ret['count'] = query.count
    ret['fileList'] = enumerate_miscfiles(session, req, query)

    return ret

def validate(req):
    # TODO: Validate a field:
    #    - existance of a filename
    #    - valid Program ID
    pass

@needs_login(superuser=True)
@templating.templated("miscfiles/miscfiles.html", with_session=True)
def save_file(session, req, formdata):
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

@templating.templated("miscfiles/detail.html", with_session=True)
def detail_miscfile(session, req, handle):
    try:
        query = session.query(MiscFile, DiskFile, File).join(DiskFile).join(File)
        try:
            meta, df, fobj = query.filter(MiscFile.id == int(handle)).one()
        except ValueError:
            # Must be a file name...
            meta, df, fobj = query.filter(File.name == handle).one()
        return dict(
            canhave = icanhave(session, req, meta),
            meta = meta,
            disk = df,
            file = fobj
            )
    except NoResultFound:
        return dict(error = "Can't find the required content")
    except MultipleResultsFound:
        return dict(error = "More than one file was found matching the provided name. This is an error!")
