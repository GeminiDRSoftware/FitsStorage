from _bz2 import BZ2Compressor

import boto3

from .fileserver import BZ2OnTheFlyDecompressor
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

from smart_open import open, s3


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


def save_mfp_file(collection, folder, formdata):
    """
    Save MiscFilesPlus file to S3

    This makes use of smart_open to stream the data out instead of staging a file
    like the existing code.  See: https://github.com/RaRe-Technologies/smart_open
    """
    filename = formdata['uploadFile'].filename
    fp = formdata['uploadFile'].file
    fullname = os.path.join(f"{collection.name}", folder.path(), filename)
    current_data = {}

    try:
        compressor = BZ2Compressor()
        session = boto3.Session(
            profile_name="localstack",
            aws_access_key_id     = 'foo', # os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key = 'foo' #  os.environ['AWS_SECRET_ACCESS_KEY'],)
        )
        url = f's3://miscfilesplus/{fullname}'
        with open(url, 'wb', transport_params={
                'client': session.client('s3', verify=False, endpoint_url='https://localhost:4566/')}) as fout:
            bytes_written = 0
            read_file = fp
            read_file.seek(0)
            dat = read_file.read(4096)
            while dat:
                cdat = compressor.compress(dat)
                bytes_written += fout.write(cdat)
                dat = read_file.read(4096)
            cdat = compressor.flush()
            if cdat:
                fout.write(cdat)
            fout.flush()
            fout.close()

        print(bytes_written)

        # formdata['uploadFile'].uploaded_file.rename_to(fullpath)
        release_date = datetime.utcnow()
        jsonurl = f's3://miscfilesplus/{fullname}.mfp.json'
        with open(jsonurl, 'w', transport_params={
                'client': session.client('s3', verify=False, endpoint_url='https://localhost:4566/')}) as meta:
            json.dump({'filename': filename,
                       'is_mfp':  'True',
                       'release':  release_date.strftime('%Y-%m-%d'),
                       'description': '',
                       'program': collection.program_id},
                      meta)
        return dict(can_add=True, actionMessage = "Uploaded to S3")
    except IOError:
        # TODO: We should log the failure
        return dict(can_add=True, errorMessage = "Error when trying to store the file on S3",
                    **current_data)

    return dict(can_add=True)


def upload_file():
    ctx = get_context()

    session = ctx.session

    formdata = get_context().get_form_data(large_file=True)

    collection_name = formdata['collection_name'].value
    file_name = formdata['uploadFile'].filename
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
            save_mfp_file(collection, folder, formdata)

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
    ctx.resp.redirect_to(f"/miscfilesplus/browse/{collection_name}/{path}/")


def get_file(collection, folders, filename):
    ctx = get_context()

    session = ctx.session

    collection = session.query(MiscFileCollection) \
        .filter(MiscFileCollection.name == collection).first()
    if not collection:
        ctx.resp.status = Return.HTTP_NOT_FOUND
        return
    else:
        # now find the parent folder
        folder = None
        for f in folders:
            folder = session.query(MiscFileFolder) \
                .filter(MiscFileFolder.folder == folder) \
                .filter(MiscFileFolder.name == f).first()
            if not folder:
                ctx.resp.status = Return.HTTP_NOT_FOUND
                return

        file = session.query(MiscFilePlus) \
                .filter(MiscFilePlus.folder == folder) \
                .filter(MiscFilePlus.collection == collection) \
                .filter(MiscFilePlus.filename == filename).first()
        if file is None:
            ctx.resp.status = Return.HTTP_NOT_FOUND
            return

        session = boto3.Session(
            profile_name="localstack",
            aws_access_key_id     = 'foo', # os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key = 'foo' #  os.environ['AWS_SECRET_ACCESS_KEY'],)
        )
        if folder:
            path = '/'.join(folders)
            url = f's3://miscfilesplus/{collection.name}/{path}/{filename}'
        else:
            url = f's3://miscfilesplus/{collection.name}/{filename}'
        with open(url, 'rb', transport_params={
                'client': session.client('s3', verify=False, endpoint_url='https://localhost:4566/')}) as fin:
            # resp.content_length = diskfile.data_size
            ctx.resp.sendfile_obj(BZ2OnTheFlyDecompressor(fin))


def delete_file(collection, folders, filename):
    ctx = get_context()

    session = ctx.session

    collection = session.query(MiscFileCollection) \
        .filter(MiscFileCollection.name == collection).first()
    if not collection:
        ctx.resp.status = Return.HTTP_NOT_FOUND
        return
    else:
        # now find the parent folder
        folder = None
        for f in folders:
            folder = session.query(MiscFileFolder) \
                .filter(MiscFileFolder.folder == folder) \
                .filter(MiscFileFolder.name == f).first()
            if not folder:
                ctx.resp.status = Return.HTTP_NOT_FOUND
                return

        file = session.query(MiscFilePlus) \
                .filter(MiscFilePlus.folder == folder) \
                .filter(MiscFilePlus.collection == collection) \
                .filter(MiscFilePlus.filename == filename).first()
        if file is None:
            ctx.resp.status = Return.HTTP_NOT_FOUND
            return

        session = boto3.Session(
            profile_name="localstack",
            aws_access_key_id     = 'foo', # os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key = 'foo' #  os.environ['AWS_SECRET_ACCESS_KEY'],)
        )
        if folder:
            path = '/'.join(folders)
            key = f'{collection.name}/{path}/{filename}'
        else:
            key = f'{collection.name}/{filename}'

        s3 = boto3.client('s3', region_name='us-east-1',
                     aws_access_key_id="foo", aws_secret_access_key="foo",
                     verify=False, endpoint_url='http://localhost:4566')
        s3.delete_object(Bucket='miscfilesplus', Key=key)

        file.delete()
        session.commit()

        if folders:
            ctx.resp.redirect_to(f"/miscfilesplus/browse/{collection.name}/{folder.path()}/")
        else:
            ctx.resp.redirect_to(f"/miscfilesplus/browse/{collection.name}/")
