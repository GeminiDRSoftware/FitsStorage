import zipfile
from tarfile import TarFile
from zipfile import ZipFile

from botocore.exceptions import ClientError
from sqlalchemy import or_

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

__all__ = ["upload_file", "miscfilesplus", "search", "add_collection",
           "add_folder", "upload_file", "get_file", "download_zip", "delete_path"]


SEARCH_LIMIT = 500


class ArchiveWrapper:
    def __init__(self):
        pass


class ArchiveInfoWrapper:
    def __init__(self):
        pass


class ZipArchive(ArchiveWrapper):
    def __init__(self, fp):
        self._zipfile = ZipFile(fp)

    def infolist(self):
        for zi in self._zipfile.infolist():
            yield ZipArchiveInfo(zi)

    def open(self, zi):
        return self._zipfile.open(zi.zipinfo)


class ZipArchiveInfo(ArchiveInfoWrapper):
    def __init__(self, zipinfo):
        self._zipinfo = zipinfo

    def is_dir(self):
        return self._zipinfo.is_dir()

    @property
    def zipinfo(self):
        return self._zipinfo

    @property
    def filename(self):
        return self._zipinfo.filename

    @property
    def date_time(self):
        return self._zipinfo.date_time

    @property
    def file_size(self):
        return self._zipinfo.file_size


class TarArchive(ArchiveWrapper):
    def __init__(self, fp):
        self._tarfile = TarFile(fileobj=fp)

    def infolist(self):
        for ti in self._tarfile.getmembers():
            yield TarArchiveInfo(ti)

    def open(self, ti):
        return self._tarfile.extractfile(ti.tarinfo)


class TarArchiveInfo(ArchiveInfoWrapper):
    def __init__(self, tarinfo):
        self._tarinfo = tarinfo

    def is_dir(self):
        return self._tarinfo.isdir()

    @property
    def tarinfo(self):
        return self._tarinfo

    @property
    def filename(self):
        return self._tarinfo.name

    @property
    def date_time(self):
        dt = datetime.fromtimestamp(self._tarinfo.mtime)
        return dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second

    @property
    def file_size(self):
        return self._tarinfo.size


def _get_archive_wrapper(filename, fp):
    if filename.lower().endswith('.zip'):
        return ZipArchive(fp)
    elif filename.lower().endswith('.tar') or filename.lower().endswith('.tar.gz') or filename.lower.endswith('.tgz'):
        return TarArchive(fp)
    return None


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

        formdata = ctx.get_form_data()

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
            folder = None  # to hold the active folder
            folder_id = None  # id as we walk the tree for the parent folder
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
                .filter(MiscFileFolder.folder_id == folder_id).order_by(_folder_order_by(formdata)).all()
            files = session.query(MiscFilePlus) \
                .filter(MiscFilePlus.collection == collection) \
                .filter(MiscFilePlus.folder_id == folder_id).order_by(_file_order_by(formdata)).all()

        url_subpath = ''
        if collection:
            url_subpath = collection.name
            if folder:
                url_subpath = f"{url_subpath}/{folder.path()}"
        return dict(collections=collections, collection=collection, folder=folder, folders=folders, files=files,
                    url_subpath=url_subpath, can_add=True)  # get_context().is_staffer)
    finally:
        if formdata and formdata.uploaded_file is not None:
            try:
                os.unlink(formdata.uploaded_file.name)
            except OSError:
                pass


def _folder_order_by(formdata):
    if 'order_by' in formdata:
        order_by = formdata['order_by'].value
        if order_by == 'name':
            return MiscFileFolder.name
        elif order_by == '-name':
            return MiscFileFolder.name.desc()
        elif order_by == 'program_id':
            return MiscFileFolder.program_id
        elif order_by == '-program_id':
            return MiscFileFolder.program_id.desc()
        elif order_by == 'release_date':
            return MiscFileFolder.release
        elif order_by == '-release_date':
            return MiscFileFolder.release.desc()
    return MiscFileFolder.name


def _file_order_by(formdata):
    if 'order_by' in formdata:
        order_by = formdata['order_by'].value
        if order_by == 'name':
            return MiscFilePlus.filename
        elif order_by == '-name':
            return MiscFilePlus.filename.desc()
        elif order_by == 'size':
            return MiscFilePlus.size
        elif order_by == '-size':
            return MiscFilePlus.size.desc()
        elif order_by == 'program_id':
            return MiscFilePlus.program_id
        elif order_by == '-program_id':
            return MiscFilePlus.program_id.desc()
        elif order_by == 'last_modified':
            return MiscFilePlus.last_modified
        elif order_by == '-last_modified':
            return MiscFilePlus.last_modified.desc()
        elif order_by == 'release_date':
            return MiscFilePlus.release
        elif order_by == '-release_date':
            return MiscFilePlus.release.desc()
    return MiscFilePlus.filename


@templating.templated("miscfilesplus/search.html")
def search():
    ctx = get_context()
    formdata = ctx.get_form_data()
    search = '%' + formdata['search_field'].value + '%'
    session = ctx.session
    # No collection, we are just going to show the list of all available collections
    collections = session.query(MiscFileCollection) \
        .filter(or_(MiscFileCollection.name.ilike(search), MiscFileCollection.program_id.ilike(search))) \
        .order_by(MiscFileCollection.name).all()
    folders= session.query(MiscFileFolder) \
        .filter(or_(MiscFileFolder.name.ilike(search), MiscFileFolder.program_id.ilike(search),
                    MiscFileFolder.description.ilike(search))).order_by(_folder_order_by(formdata)).all()
    files = session.query(MiscFilePlus) \
        .filter(or_(MiscFilePlus.filename.ilike(search), MiscFilePlus.program_id.ilike(search),
                    MiscFilePlus.description.ilike(search))).order_by(_file_order_by(formdata)).all()

    return dict(collections=collections, folders=folders, files=files, search_field=formdata['search_field'].value)  # get_context().is_staffer)


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
    ctx = get_context()

    session = ctx.session

    formdata = get_context().get_form_data(large_file=True)

    collection_name = formdata['collection_name'].value
    folder_name = formdata['folder_name'].value
    if 'path' in formdata:
        path = formdata['path'].value
    else:
        path = None
    if 'folder_description' in formdata:
        description = formdata['folder_description'].value
    else:
        description = None
    if 'folder_program_id' in formdata:
        program_id = formdata['folder_program_id'].value
    else:
        program_id = ''

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
    else:
        ctx.resp.status = Return.HTTP_BAD_REQUEST
        return

    _add_folder(ctx, session, collection, folder, folder_name, program_id, description)

    if path is None:
        ctx.resp.redirect_to(f"/miscfilesplus/browse/{collection_name}/")
    else:
        ctx.resp.redirect_to(f"/miscfilesplus/browse/{collection_name}/{path}/")


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


def _add_folder(ctx, session, collection, folder, folder_name, program_id, description):
    mff = MiscFileFolder()
    mff.name = folder_name
    mff.collection = collection
    mff.folder = folder
    mff.program_id = program_id
    mff.description = description
    mff.release = datetime.now()
    session.add(mff)
    session.commit()
    return mff


def _upload_zip_file(ctx, session, filename, fp, collection, folder, program_id, release_date, description):
    aw = _get_archive_wrapper(filename, fp)
    for ai in aw.infolist():
        # if we need to create a path below the current folder, check and do so here
        # then track the final "zipfolder" as the location to put the new folder or file
        # and use the final element in the path as the filename
        zipfolder = folder
        zippath = ai.filename.rstrip('/').split('/')
        zipfilename = zippath[-1]
        for zif in zippath[:-1]:
            zipf = session.query(MiscFileFolder).filter(MiscFileFolder.folder == zipfolder,
                                                        MiscFileFolder.name == zif).first()
            if zipf is None:
                zipfolder = _add_folder(ctx, session, collection, zipfolder, zif, program_id, description)
            else:
                zipfolder = zipf

        if ai.is_dir():
            zipf = session.query(MiscFileFolder).filter(MiscFileFolder.folder == zipfolder,
                                                        MiscFileFolder.name == zipfilename).first()
            if zipf is None:
                zipf = session.query(MiscFilePlus).filter(MiscFilePlus.folder == zipfolder,
                                                          MiscFilePlus.filename == zipfilename).first()
                if zipf is not None:
                    # file already exists with that name
                    ctx.resp.status = Return.HTTP_BAD_REQUEST
                    return
                _add_folder(ctx, session, collection, zipfolder, zipfilename, program_id, description)
        else:
            with aw.open(ai) as zdata:
                _save_mfp_file(collection, zipfolder, zdata, zipfilename)
                mfp = MiscFilePlus()
                mfp.folder = zipfolder
                mfp.filename = zipfilename
                mfp.collection = collection
                if program_id:
                    mfp.program_id = program_id
                else:
                    mfp.program_id = collection.program_id
                if release_date:
                    mfp.release = release_date
                else:
                    mfp.release = datetime.utcnow()
                mfp.description = description
                if ai.date_time:
                    mfp.last_modified = datetime(ai.date_time[0], ai.date_time[1], ai.date_time[2],
                                                 ai.date_time[3], ai.date_time[4], ai.date_time[5])
                mfp.size = ai.file_size
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
    if 'file_description' in formdata:
        description = formdata['file_description'].value
    else:
        description = None
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

            if file_name.lower().endswith('zip') or file_name.lower().endswith('.tgz') \
                    or file_name.lower().endswith('.tar.gz') or file_name.lower().endswith('.tar'):
                _upload_zip_file(ctx, session, file_name, formdata['file'].file, collection, folder, program_id,
                                 release_date, '')  #formdata['description'].value)
            else:
                # logic to save to S3 goes in here
                fp = formdata.fp
                _save_mfp_file(collection, folder, fp, file_name)

                mfp = MiscFilePlus()
                mfp.folder = folder
                mfp.filename = file_name
                mfp.collection = collection
                mfp.description = description
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


from io import RawIOBase

class UnseekableStream(RawIOBase):
    """
    Utility class for download_zip.

    This allows us to stream a zip on the fly out through the
    wsgi response.  It is therefore very memory efficient and
    also avoids staging S3 files onto disk.
    """
    def __init__(self):
        self._buffer = b''

    def writable(self):
        return True

    def write(self, b):
        if self.closed:
            raise ValueError('Stream was closed!')
        self._buffer += b
        return len(b)

    def get(self):
        chunk = self._buffer
        self._buffer = b''
        return chunk


def download_zip():
    ctx = get_context()

    session = ctx.session

    formdata = get_context().get_form_data(large_file=True)

    # Helper to make a list of file ids from the input form
    # This handles empty inputs, single values and string conversions
    # to always produce a list of integer values
    def make_list(formdata, field):
        if field not in formdata:
            return []
        data = formdata[field]
        if isinstance(data, list):
            retval = []
            for f in data:
                retval.append(int(f.value))
            return retval
        else:
            return [int(data.value)]

    files = make_list(formdata, 'files')

    def iter_zip(session, stream):
        # This serves as an iterator pulling chunks out of a streaming ZipFile
        # as it builds, but also feeding the ZipFile chunks of data from the s3
        # stored requested files as it goes.  The stream passed in is an
        # UnseekableStream as defined above.  This keeps our data off disk and
        # our memory footprint small.
        with ZipFile(stream, mode='w') as zf:
            for file in session.query(MiscFilePlus).filter(MiscFilePlus.id.in_(files)):
                zi = zipfile.ZipInfo(filename=file.filename)
                with zf.open(zi, mode='w') as dest:
                    collection = file.collection
                    folder = file.folder

                    boto3_session = boto3.Session(
                        **_get_boto3_session_kwargs()
                    )
                    if folder:
                        path = folder.path()
                        url = f's3://miscfilesplus/{collection.name}/{path}/{file.filename}'
                    else:
                        url = f's3://miscfilesplus/{collection.name}/{file.filename}'
                    with open(url, 'rb', transport_params={
                        'client': boto3_session.client('s3', **_get_session_client_kwargs())}) as fin:
                        decomp = BZ2OnTheFlyDecompressor(fin)
                        chunk = decomp.read(8192)
                        while chunk:
                            dest.write(chunk)
                            chunk = decomp.read(8192)
                            yield stream.get()
        # done, get any last data
        yield stream.get()

    ustream = UnseekableStream()

    ctx.resp.content_disposition = f'attachment; filename=gemini_files.zip'
    ctx.resp.append_iterable(iter_zip(session, ustream))


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
