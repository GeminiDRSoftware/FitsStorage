from ..orm.miscfile import MiscFile, normalize_diskname
from ..orm.diskfile import DiskFile
from ..orm.file     import File
from ..orm          import NoResultFound, MultipleResultsFound

from . import templating

from ..fits_storage_config import upload_staging_path, api_backend_location

from ..gemini_metadata_utils import GeminiProgram
from ..orm.miscfile_plus import MiscFileCollection, MiscFileFolder, MiscFilePlus

from ..utils.api import ApiProxy, ApiProxyError
from ..utils.userprogram import icanhave
from ..utils.web import get_context, Return

from .user import needs_login

import dateutil
import json
import os
import stat
from datetime import datetime, timedelta

from cgi import parse_header

SEARCH_LIMIT = 500


@templating.templated("miscfilesplus/miscfilesplus.html")
def miscfilesplus(collection=None, folders=None, file=None):
    formdata = None
    try:
#        if len(things) == 1 and things[0] == 'validate_add':
#            return validate()

        ctx = get_context()
        env = ctx.env

        # formdata = ctx.get_form_data(large_file=True)

        session = ctx.session
        if collection is None:
            # No collection, we are just going to show the list of all available collections
            collections = session.query(MiscFileCollection).order_by(MiscFileCollection.name).all()
            collection = None
            folder = None
            folders = None
            files = None
        else:
            # First, get the active collection (first part of the path)
            collections = None
            collection = session.query(MiscFileCollection) \
                .filter(MiscFileCollection.name == collection).one()

            # Now iterate over the list of folders from the URL to reach the active folder
            folder = None # to hold the active folder
            folder_id = None # id as we walk the tree for the parent folder
            if folders:
                for folder_name in folders:
                    folder = session.query(MiscFileFolder) \
                        .filter(MiscFileFolder.collection_id == collection.id) \
                        .filter(MiscFileFolder.folder_id == folder_id) \
                        .filter(MiscFileFolder.name == folder_name).first()
                    if folder:
                        folder_id = folder.id

            # Now get all the contained subfolders and files in our folder
            folders = session.query(MiscFileFolder) \
                .filter(MiscFileFolder.collection == collection) \
                .filter(MiscFileFolder.folder_id == folder_id).all()
            files = session.query(MiscFilePlus) \
                .filter(MiscFilePlus.collection == collection) \
                .filter(MiscFilePlus.folder_id == folder_id).all()

        return dict(collections=collections, collection=collection, folder=folder, folders=folders, files=files,
                    can_add=True)  # get_context().is_staffer)
    finally:
        if formdata and formdata.uploaded_file is not None:
            try:
                os.unlink(formdata.uploaded_file.name)
            except OSError:
                pass


def add_collection():
    try:
        ctx = get_context()

        # if handle is None and 'upload' in formdata:
        #     return save_file_fixed(get_context()._env)
        session = ctx.session

        formdata = get_context().get_form_data(large_file=True)

        collection_name = formdata['collection_name'].value
        program_id = formdata['program_id'].value
        description = formdata['description'].value

        if collection_name:
            collection = session.query(MiscFileCollection) \
                .filter(MiscFileCollection.name == collection_name).first()
            if collection:
                ctx.resp.status = Return.HTTP_BAD_REQUEST
                return
            else:
                mfc = MiscFileCollection()
                mfc.name = collection_name
                mfc.description = description
                mfc.program_id = program_id
                session.add(mfc)
                session.commit()

        ctx.resp.redirect_to("/miscfilesplus/")
    finally:
        if formdata and formdata.uploaded_file is not None:
            try:
                os.unlink(formdata.uploaded_file.name)
            except OSError:
                pass


def add_folder():
    try:
        ctx = get_context()

        # if handle is None and 'upload' in formdata:
        #     return save_file_fixed(get_context()._env)
        session = ctx.session

        formdata = get_context().get_form_data(large_file=True)

        collection_name = formdata['collection_name'].value
        folder_name = formdata['folder_name'].value
        if 'path' in formdata:
            path = formdata['path'].value
        else:
            path = None
        folder = None

        if collection_name:
            collection = session.query(MiscFileCollection) \
                .filter(MiscFileCollection.name == collection_name).first()
            if not collection:
                ctx.resp.status = Return.HTTP_BAD_REQUEST
                return
            else:
                # now find the parent folder
                if path:
                    for f in path.split('/'):
                        folder = session.query(MiscFileFolder) \
                            .filter(MiscFileFolder.folder == folder) \
                            .filter(MiscFileFolder.name == f).first()
                        if not folder:
                            ctx.resp.status = Return.HTTP_BAD_REQUEST
                            return
                mff = MiscFileFolder()
                mff.name = folder_name
                mff.collection = collection
                mff.folder = folder
                mff.program_id = collection.program_id
                mff.description = ''
                mff.release = datetime.now()
                session.add(mff)
                session.commit()
        else:
            ctx.resp.status = Return.HTTP_BAD_REQUEST
            return

        if path is None:
            path = ''
        ctx.resp.redirect_to(f"/miscfilesplus/{path}")
    finally:
        if formdata and formdata.uploaded_file is not None:
            try:
                os.unlink(formdata.uploaded_file.name)
            except OSError:
                pass


def save_mfp_file(session, formdata):
    # fileitem = formdata.uploaded_file
    fileitem = formdata['uploadFile'].uploaded_file
    localfilename = normalize_diskname(fileitem.name)
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

    uploadRelease = formdata.getvalue('uploadRelease', '').strip()
    if uploadRelease == 'default':
        release_date = datetime.now() + timedelta(days=540) # Now + 18 pseudo-months
    elif uploadRelease == 'now':
        release_date = datetime.now()
    else:
        try:
            release_date = string_to_date(formdata.getvalue('arbRelease', '').strip())
        except (ValueError, KeyError):
            return dict(can_add=True, errorMessage = "Wrong value for release date",
                        **current_data)

    try:
        with open(fullpath, 'wb') as staging_file:
            read_file = formdata['uploadFile'].file
            read_file.seek(0)
            dat = read_file.read(4096)
            while dat:
                staging_file.write(dat)
                dat = read_file.read(4096)
            staging_file.flush()
            staging_file.close()
        # formdata['uploadFile'].uploaded_file.rename_to(fullpath)
        os.chmod(fullpath, stat.S_IRUSR|stat.S_IRGRP|stat.S_IROTH)
        with open(jsonpath, 'w') as meta:
            json.dump({'filename': fileitem.name,
                       'is_misc':  'True',
                       'release':  release_date.strftime('%Y-%m-%d'),
                       'description': formdata['uploadDesc'].value,
                       'program': formdata['uploadProg'].value},
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


def upload_file():
    try:
        ctx = get_context()

        # if handle is None and 'upload' in formdata:
        #     return save_file_fixed(get_context()._env)
        session = ctx.session

        formdata = get_context().get_form_data(large_file=True)

        collection_name = formdata['collection_name'].value
        file_name = formdata['upload_file'].value
        if 'path' in formdata:
            path = formdata['path'].value
        else:
            path = None
        folder = None

        if collection_name:
            collection = session.query(MiscFileCollection) \
                .filter(MiscFileCollection.name == collection_name).first()
            if not collection:
                ctx.resp.status = Return.HTTP_BAD_REQUEST
                return
            else:
                # now find the parent folder
                if path:
                    for f in path.split('/'):
                        folder = session.query(MiscFileFolder) \
                            .filter(MiscFileFolder.folder == folder) \
                            .filter(MiscFileFolder.name == f).first()
                        if not folder:
                            ctx.resp.status = Return.HTTP_BAD_REQUEST
                            return

                # logic to save to S3 goes in here
                # save_mfp_file(session, formdata)

                mfp = MiscFilePlus()
                mfp.folder = folder
                mfp.filename = file_name
                mfp.collection = collection
                mfp.program_id = collection.program_id
                mfp.release = datetime.now()
                mfp.description = ''
                session.add(mfp)
                session.commit()
        else:
            ctx.resp.status = Return.HTTP_BAD_REQUEST
            return

        if path is None:
            path = ''
        ctx.resp.redirect_to(f"/miscfilesplus/{path}")
    finally:
        if formdata and formdata.uploaded_file is not None:
            try:
                os.unlink(formdata.uploaded_file.name)
            except OSError:
                pass
