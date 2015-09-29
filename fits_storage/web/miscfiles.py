from ..orm.miscfile import MiscFile, normalize_diskname
from ..orm.diskfile import DiskFile
from ..orm.file     import File

from . import templating

from ..fits_storage_config import upload_staging_path, api_backend_location

from ..utils.api import ApiProxy, ApiProxyError
from ..utils.userprogram import icanhave

from mod_python import util
import json
import os
from shutil import copyfileobj
from datetime import datetime, timedelta

@templating.templated("miscfiles/miscfiles.html", with_session=True)
def miscfiles(session, req):
    formdata = util.FieldStorage(req)

    if 'search' in formdata:
        return search_miscfiles(session, req, formdata)

    # TODO: Only Gemini staff should have access to this
    if 'upload' in formdata:
        return save_file(session, req, formdata)

    return {}

def enumerate_miscfiles(session, req, query):
    for misc, file in query:
        yield icanhave(session, req, misc), misc, file

def search_miscfiles(session, req, formdata):
    ret = {}
    query = session.query(MiscFile, File).join(DiskFile).join(File)

    message = []

    name = formdata['name'].strip()
    # Make sure there are no '&' in the keywords
    keyw = ' '.join(formdata['keyw'].split('&')).strip()
    prog = formdata['prog'].strip()

    if name:
        query = query.filter(File.name.like('%' + name + '%'))
        ret['searchName'] = name

    if keyw:
        query = query.filter(MiscFile.description.match(' & '.join(keyw.split())))
        ret['searchKeyw'] = keyw

    if prog:
        query = query.filter(MiscFile.program_id.like('%' + prog + '%'))
        ret['searchProg'] = prog

    query = query.order_by(File.name).limit(500)
    ret['count'] = query.count
    ret['fileList'] = enumerate_miscfiles(session, req, query)

    return ret

def validate(req):
    # TODO: Validate a field:
    #    - existance of a filename
    #    - valid Program ID
    pass

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
        return dict(errorMessage = "Wrong value for release date",
                    **current_data)

    try:
        with open(fullpath, 'w') as dst, open(jsonpath, 'w') as meta:
            copyfileobj(fileitem.file, dst)
            json.dump({'filename': fileitem.filename,
                       'is_misc':  'True',
                       'release':  release_date.strftime('%Y-%m-%d'),
                       'description': formdata.get('uploadDesc', None),
                       'program': formdata.get('uploadProg', None)},
                     meta)
    except IOError:
        cleanup()
        # TODO: We should log the failure
        return dict(errorMessage = "Error when trying to save the file",
                    **current_data)

    try:
        proxy = ApiProxy(api_backend_location)
        result = proxy.ingest_upload(filename=localfilename)
        return dict(actionMessage = "Ingested with result: " + str(result))
    except ApiProxyError:
        cleanup()
        return dict(errorMessage = "Error when trying to queue the file for ingestion",
                    **current_data)

    return {}
