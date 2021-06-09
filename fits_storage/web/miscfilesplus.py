from zipfile import ZipFile

from botocore.exceptions import ClientError

import fits_storage.fits_storage_config as fsc

from _bz2 import BZ2Compressor

import boto3

from .fileserver import BZ2OnTheFlyDecompressor
from . import templating

from ..orm.miscfile_plus import MiscFileCollection, MiscFileFolder, MiscFilePlus

from ..utils.web import get_context, Return

import json
import os
from datetime import datetime, timedelta

from smart_open import open


SEARCH_LIMIT = 500


def _get_boto3_client_kwargs():
    retval = {
        'region_name': fsc.mfp_aws_region_name,
        'aws_access_key_id': fsc.mfp_aws_access_key_id,
        'aws_secret_access_key': fsc.mfp_aws_secret_access_key,
        'verify': fsc.mfp_aws_verify,
    }
    if fsc.mfp_aws_endpoint_url:
        retval['endpoint_url'] = fsc.mfp_aws_endpoint_url
    return retval


def _get_boto3_session_kwargs():
    retval = {
        'aws_access_key_id': fsc.mfp_aws_access_key_id,
        'aws_secret_access_key': fsc.mfp_aws_secret_access_key
    }
    if fsc.mfp_aws_profile_name:
        retval['profile_name'] = fsc.mfp_aws_profile_name
    return retval


def _get_session_client_kwargs():
    retval = {}
    if fsc.mfp_aws_verify:
        retval['verify'] = True
    elif fsc.mfp_aws_verify is False:
        retval['verify'] = False
    if fsc.mfp_aws_endpoint_url:
        retval['endpoint_url'] = fsc.mfp_aws_endpoint_url
    return retval


@templating.templated("miscfilesplus/miscfilesplus.html")
def miscfilesplus(collection=None, folders=None):
    formdata = None
    try:
        ctx = get_context()

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
            ctx.resp.redirect_to(f"/miscfilesplus/browse/{collection_name}/")
        else:
            ctx.resp.redirect_to(f"/miscfilesplus/browse/{collection_name}/{path}/")
    finally:
        if formdata and formdata.uploaded_file is not None:
            try:
                os.unlink(formdata.uploaded_file.name)
            except OSError:
                pass


def _save_mfp_file(collection, folder, fp, filename):
    """
    Save MiscFilesPlus file to S3

    This makes use of smart_open to stream the data out instead of staging a file
    like the existing code.  See: https://github.com/RaRe-Technologies/smart_open
    """
    if folder:
        fullname = f"{collection.name}/{folder.path()}/{filename}"
    else:
        fullname = f"{collection.name}/{filename}"
    current_data = {}

    try:
        compressor = BZ2Compressor()
        session = boto3.Session(
            **_get_boto3_session_kwargs()
        )
        client = session.client('s3', **_get_session_client_kwargs())
        try:
            client.head_bucket(Bucket='miscfilesplus')
        except ClientError:
            client.create_bucket(Bucket='miscfilesplus')
        url = f's3://miscfilesplus/{fullname}'
        with open(url, 'wb', transport_params={
                'client': session.client('s3', **_get_session_client_kwargs())}) as fout:
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

        release_date = datetime.utcnow()
        jsonurl = f's3://miscfilesplus/{fullname}.mfp.json'
        with open(jsonurl, 'w', transport_params={
                'client': session.client('s3', **_get_session_client_kwargs())}) as meta:
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


def _upload_zip_file(session, fp, collection, folder, program_id, release_date, description):
    with ZipFile(fp) as zf:
        for zi in zf.infolist():
            with zf.open(zi) as zdata:
                _save_mfp_file(collection, folder, zdata, zi.filename)
                mfp = MiscFilePlus()
                mfp.folder = folder
                mfp.filename = zi.filename
                mfp.collection = collection
                if program_id:
                    mfp.program_id = program_id
                else:
                    mfp.program_id = collection.program_id
                if release_date:
                    mfp.release = release_date
                else:
                    mfp.release = datetime.now()
                mfp.description = description
                session.add(mfp)
                session.commit()


def upload_file():
    ctx = get_context()

    session = ctx.session

    formdata = get_context().get_form_data(large_file=True)

    collection_name = formdata['collection_name'].value
    file_name = formdata['file'].filename
    if not file_name:
        ctx.resp.status = Return.HTTP_BAD_REQUEST
        return
    if file_name.endswith('.mfp.json'):
        # reserved suffix
        ctx.resp.status = Return.HTTP_BAD_REQUEST
        return
    if 'path' in formdata:
        path = formdata['path'].value
    else:
        path = None
    if 'file_program_id' in formdata:
        program_id = formdata['file_program_id'].value
    else:
        program_id = None
    if 'file_release_date' in formdata:
        release_date = formdata['file_release_date'].value
        try:
            release_date = datetime.strptime(release_date, '%Y%m%d')
        except:
            release_date = None
    else:
        release_date = None
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

            if file_name.lower().endswith('zip'):
                _upload_zip_file(session, formdata['file'].file, collection, folder, program_id,
                                 release_date, '')  #formdata['description'].value)
            else:
                # logic to save to S3 goes in here
                fp = formdata.fp
                _save_mfp_file(collection, folder, fp, file_name)

                mfp = MiscFilePlus()
                mfp.folder = folder
                mfp.filename = file_name
                mfp.collection = collection
                if program_id:
                    mfp.program_id = program_id
                else:
                    mfp.program_id = collection.program_id
                if release_date:
                    mfp.release = release_date
                else:
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
            **_get_boto3_session_kwargs()
        )
        if folder:
            path = '/'.join(folders)
            url = f's3://miscfilesplus/{collection.name}/{path}/{filename}'
        else:
            url = f's3://miscfilesplus/{collection.name}/{filename}'
        with open(url, 'rb', transport_params={
                'client': session.client('s3', **_get_session_client_kwargs())}) as fin:
            # resp.content_length = diskfile.data_size
            ctx.resp.sendfile_obj(BZ2OnTheFlyDecompressor(fin))


def _delete_folder_recursive(session, folder):
    for child_file in session.query(MiscFilePlus) \
            .filter(MiscFilePlus.folder == folder).all():
        session.delete(child_file)
    for child_folder in session.query(MiscFileFolder) \
            .filter(MiscFileFolder.folder == folder).all():
        _delete_folder_recursive(session, child_folder)
    session.delete(folder)


def delete_path(collection, path):
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
        for f in path[:-1]:
            folder = session.query(MiscFileFolder) \
                .filter(MiscFileFolder.folder == folder) \
                .filter(MiscFileFolder.name == f).first()
            if not folder:
                ctx.resp.status = Return.HTTP_NOT_FOUND
                return

        name = path[-1]
        parent = folder
        checkfolder = session.query(MiscFileFolder) \
            .filter(MiscFileFolder.folder == parent) \
            .filter(MiscFileFolder.name == name).first()
        if checkfolder:
            _delete_folder_recursive(session, checkfolder)
            session.commit()
        else:
            # should be a file, find it, remove it from S3 and delete the record
            checkfile = session.query(MiscFilePlus) \
                .filter(MiscFilePlus.folder == parent) \
                .filter(MiscFilePlus.filename == name).first()
            if checkfile:
                path = '/'.join(path)
                key = f'{collection.name}/{path}/{name}'

                s3 = boto3.client('s3', **_get_boto3_client_kwargs())
                s3.delete_object(Bucket='miscfilesplus', Key=key)
                s3.delete_object(Bucket='miscfilesplus', Key=f"{key}.mfp.json")

                session.delete(checkfile)
                session.commit()
            else:
                ctx.resp.status = Return.HTTP_NOT_FOUND
                return
        if parent:
            ctx.resp.redirect_to(f"/miscfilesplus/browse/{collection.name}/{parent.path()}/")
        else:
            ctx.resp.redirect_to(f"/miscfilesplus/browse/{collection.name}/")
