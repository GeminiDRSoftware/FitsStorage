"""
This module contains utility functions for interacting with AWS S3
"""

import os

from fits_storage.config import get_config
from fits_storage.logger_dummy import DummyLogger

from fits_storage.core.hashes import md5sum
from contextlib import contextmanager

import boto3
from boto3.s3.transfer import S3UploadFailedError, RetriesExceededError
from botocore.exceptions import ClientError
import logging
from tempfile import mkstemp


class DownloadError(Exception):
    pass


boto3.set_stream_logger(level=logging.CRITICAL)
logging.getLogger('boto').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('utils').setLevel(logging.CRITICAL)
logging.getLogger('tasks').setLevel(logging.CRITICAL)
logging.getLogger('futures').setLevel(logging.CRITICAL)


class Boto3Helper(object):
    def __init__(self, bucket_name=None, logger=None, access_key=None,
                 secret_key=None, s3_staging_dir=None, storage_root=None):
        fsc = get_config()
        self.l = logger if logger is not None else DummyLogger()
        self.b = None
        self.b_name = bucket_name if bucket_name is not None else \
            fsc.s3_bucket_name
        self.access_key = access_key if access_key is not None else \
            fsc.aws_access_key
        self.secret_key = secret_key if secret_key is not None else \
            fsc.aws_secret_key
        self.s3_staging_dir = s3_staging_dir if s3_staging_dir is not None \
            else fsc.s3_staging_dir
        self.storage_root = storage_root if storage_root is not None \
            else fsc.storage_root

    @property
    def session(self):
        if boto3.DEFAULT_SESSION is None:
            boto3.setup_default_session(aws_access_key_id=self.access_key,
                                        aws_secret_access_key=self.secret_key)
        return boto3.DEFAULT_SESSION

    @property
    def s3_client(self):
        return self.session.client('s3')

    @property
    def s3(self):
        return self.session.resource('s3')

    @property
    def bucket(self):
        if self.b is None:
            self.b = self.s3.Bucket(self.b_name)
        return self.b

    def list_keys(self):
        return self.bucket.objects.all()

    def list_keys_with_prefix(self, prefix):
        return self.bucket.objects.filter(Prefix=prefix)

    def key_names(self):
        return [obj.key for obj in self.list_keys()]

    def key_names_with_prefix(self, prefix):
        return [obj.key for obj in self.list_keys_with_prefix(prefix)]

    def exists_key(self, key):
        try:
            if isinstance(key, str):
                key = self.bucket.Object(key)
            key.expires
            return True
        except ClientError:
            return False

    def get_key(self, keyname):
        return self.bucket.Object(keyname)

    def delete_key(self, key):
        try:
            self.s3_client.delete_object(Bucket=self.b_name, Key=key)
            return True
        except ClientError:
            return False

    def get_md5(self, key):
        """
        Get the MD5 that the S3 metadata has for this key.
        """
        if isinstance(key, str):
            key = self.get_key(key)

        try:
            return key.metadata['md5']
        except KeyError:
            # Old object, we haven't re-calculated the MD5 yet
            return None

    def get_size(self, key):
        return key.content_length

    def get_name(self, obj):
        return obj.key

    def set_metadata(self, keyname, **kw):
        obj = self.get_key(keyname)
        md = obj.metadata
        md.update(kw)
        bn = self.bucket.name
        self.s3_client.copy_object(Bucket=bn, Key=keyname,
                                   CopySource=f'{bn}/{keyname}',
                                   MetadataDirective='REPLACE', Metadata=md)

    def copy(self, keyname, to_bucket):
        # As of 2024-Oct seems like if this does a multipart copy, then
        # it doesn't copy the metadata. So we need to specify that manually.
        md = self.get_key(keyname).metadata

        # This is a boto3 "managed copy" that will do a simple copy or a
        # multipart copy as required.
        copy_source = {
            'Bucket': self.bucket.name,
            'Key': keyname
        }

        self.s3_client.copy(copy_source, to_bucket, keyname,
                            ExtraArgs={'Metadata': md})

    def upload_file(self, keyname, filename, extra_meta={}):
        md5 = md5sum(filename)
        try:
            meta = dict(md5=md5)
            meta.update(extra_meta)
            self.s3_client.upload_file(filename, self.bucket.name, keyname,
                                       ExtraArgs={'Metadata': meta})
        except S3UploadFailedError:
            self.l.error("S3 Upload Failed", exc_info=True)
            return None

        obj = self.get_key(keyname)
        # Make sure that the object is available before returning the key
        obj.wait_until_exists()

        return obj

    def fetch_to_storageroot(self, keyname, fullpath=None, skip_tests=False):
        """
        Fetch the file from s3 and put it in the storage_root directory.
        Do some validation, and re-try as appropriate
        Return True if succeeded, False otherwise
        """
        if not fullpath:
            fullpath = os.path.join(self.storage_root, keyname)

        # Check if the file already exists in the storage area, remove it if so
        if os.path.exists(fullpath):
            self.l.warning("File already exists at S3 download location: %s. "
                           "Will delete it first.", fullpath)
            try:
                os.unlink(fullpath)
            except:
                self.l.error("Unable to delete %s which is in the way of the "
                             "S3 download", fullpath)
                return False

        try:
            self.s3_client.download_file(self.bucket.name, keyname, fullpath)
        except RetriesExceededError:
            self.l.error("Retries Exceeded", exc_info=True)
            return False

        if skip_tests:
            return True

        key = self.get_key(keyname)
        # Check size and md5
        filesize = os.path.getsize(fullpath)
        s3size = self.get_size(key)
        if filesize == s3size:
            # It's the right size, check the md5
            filemd5 = md5sum(fullpath)
            s3md5 = self.get_md5(key)
            if filemd5 == s3md5:
                # md5 matches
                self.l.debug("Downloaded file from S3 successfully")
                return True
            else:
                # Size is OK, but md5 is not
                self.l.error("Problem fetching %s from S3 - size OK, but md5 "
                             "mismatch - file: %s; key: %s",
                             keyname, filemd5, s3md5)
                return False
        else:
            # Didn't get enough bytes
            self.l.error("Problem fetching %s from S3 - size mismatch - "
                         "file: %s; key: %s", keyname, filesize, s3size)
            return False

    @contextmanager
    def fetch_temporary(self, keyname, **kwarg):
        _, fullpath = mkstemp(dir=self.s3_staging_dir)
        try:
            if not self.fetch_to_storageroot(keyname, fullpath, **kwarg):
                raise DownloadError("Could not download the file")
            yield open(fullpath, mode='rb')
        finally:
            if os.path.exists(fullpath):
                os.unlink(fullpath)


def get_helper(*args, **kwargs):
    return Boto3Helper(*args, **kwargs)
