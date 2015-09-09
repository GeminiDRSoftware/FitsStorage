"""
This module contains utility functions for interacting with AWS S3
"""

import os
import sys
import socket
import traceback
from time import sleep

from ..fits_storage_config import storage_root
from ..logger import logger
from .hashes import md5sum

class S3Helper(object):
    def __init__(self, logger_ = None):
        # This will hold the bucket
        self.l = logger_ if logger_ is not None else logger
        self.b = None

    @property
    def bucket(self):
        raise NotImplementedError("This method needs to be implemented by subclasses")

    @property
    def list_keys(self):
        raise NotImplementedError("This method needs to be implemented by subclasses")

    @property
    def key_names(self):
        raise NotImplementedError("This method needs to be implemented by subclasses")

    def exists_key(self, key):
        raise NotImplementedError("This method needs to be implemented by subclasses")

    def get_key(self, keyname):
        raise NotImplementedError("This method needs to be implemented by subclasses")

    def get_md5(self, key):
        """
        Get the MD5 that the S3 server hs for this key.
        Simply strips quotes from the etag value.
        """
        if isinstance(key, (str, unicode)):
            key = self.get_key(key)

        return self.get_etag(key).replace('"', '')

    def get_etag(self, key):
        raise NotImplementedError("This method needs to be implemented by subclasses")

    def get_size(self, key):
        raise NotImplementedError("This method needs to be implemented by subclasses")

    def get_name(self, key):
        raise NotImplementedError("This method needs to be implemented by subclasses")

    def get_as_string(self, key):
        raise NotImplementedError("This method needs to be implemented by subclasses")

    def store_file_to_keyname(self, keyname, path):
        raise NotImplementedError("This method needs to be implemented by subclasses")

    def upload_file(self, keyname, filename):
        """
        Upload the file at filename to the S3 bucket, calling it keyname
        """
        self.l.debug("Creating key: %s", keyname)
        self.l.info("Uploading %s to S3 as %s", filename, keyname )
        num = 0
        ok = False
        while num < 5 and not ok:
            num += 1
            try:
                self.store_file_to_keyname(keyname, filename)
                self.l.info("Uploaded %s OK on try %d", filename, num)
                ok = True
            except:
                self.l.debug("Upload try %d appeared to fail", num)
                self.l.debug("Exception is: %s %s %s", sys.exc_info()[0], sys.exc_info()[1], traceback.format_tb(sys.exc_info()[2]))
        if num == 5 and not ok:
            self.l.error("Gave up trying to upload %s to S3", filename)
        return ok

    def fetch_file(self, keyname, filename, path):
        raise NotImplementedError("This method needs to be implemented by subclasses")

    def fetch_to_staging(self, path, filename, key=None, fullpath=None):
        """
        Fetch the file from s3 and put it in the storage_root directory.
        Do some validation, and re-try as appropriate
        Return True if suceeded, False otherwise
        """

        # Make the full path of the destination file if we were not given one
        if fullpath is None:
            fullpath = os.path.join(storage_root, filename)

        # Check if the file already exists in the staging area, remove it if so
        if os.path.exists(fullpath):
            self.l.warning("File already exists at S3 download location: %s. Will delete it first.", fullpath)
            try:
                os.unlink(fullpath)
            except:
                self.l.error("Unable to delete %s which is in the way of the S3 download", fullpath)

        # Try up to 5 times. Have seen socket.error raised
        tries = 0
        gotit = False
        keyname = os.path.join(path, filename)
        while (not gotit) and (tries < 5):
            tries += 1
            self.l.debug("Fetching %s to s3_staging_area, try %d", filename, tries)

            try:
                key = self.fetch_file(key or keyname, filename, fullpath)
            except socket.error:
                # OK, we got a socket error.
                self.l.debug("Socket Error fetching %s from S3 - will retry, tries=%d", filename, tries)
                self.l.debug("Socket Error details: %s : %s... %s", sys.exc_info()[0], sys.exc_info()[1],
                                    traceback.format_tb(sys.exc_info()[2]))
                sleep(10)

                # Nullify the key object - seems like if it fails getting a new key is necessary
                key = None

                # Remove any partial file we got downloaded
                try:
                    os.unlink(fullpath)
                except:
                    pass

            # Did we get anything?
            if os.access(fullpath, os.F_OK):
                # Check size and md5
                filesize = os.path.getsize(fullpath)
                s3size = self.get_size(key)
                if filesize == s3size:
                    # It's the right size, check the md5
                    filemd5 = md5sum(fullpath)
                    s3md5 = self.get_md5(key)
                    if filemd5 == s3md5:
                        # md5 matches
                        gotit = True
                    else:
                        # Size is OK, but md5 is not
                        gotit = False
                        self.l.debug("Problem fetching %s from S3 - size OK, but md5 mismatch - file: %s; key: %s",
                                        filename, filemd5, s3md5)
                        sleep(10)
                else:
                    # Didn't get enough bytes
                    gotit = False
                    self.l.debug("Problem fetching %s from S3 - size mismatch - file: %s; key: %s", filename, filesize, s3size)
                    sleep(10)
            else:
                # file is not accessible
                gotit = False

        if gotit:
            self.l.debug("Downloaded file from S3 sucessfully")
            return True
        else:
            self.l.error("Failed to sucessfully download file %s from S3. Giving up.", filename)
            return False

from ..fits_storage_config import aws_access_key, aws_secret_key, s3_bucket_name
try:
    from boto3.session import Session
    from botocore.exceptions import ClientError
    import shutil

    class Boto3Helper(S3Helper):
        @property
        def s3(self):
             return Session(aws_access_key_id     = aws_access_key,
                            aws_secret_access_key = aws_secret_key).resource('s3')

        @property
        def bucket(self):
            if self.b is None:
                self.b = self.s3.Bucket(s3_bucket_name)
            return self.b

        def list_keys(self):
            return self.bucket.objects.all()

        def key_names(self):
            return [obj.key for obj in self.list_keys()]

        def exists_key(self, key):
            try:
                if isinstance(key, (str, unicode)):
                    key = self.bucket.Object(key)
                key.expires
                return True
            except ClientError:
                return False

        def get_key(self, keyname):
            return self.bucket.Object(keyname)

        def get_etag(self, key):
            return key.e_tag

        def get_size(self, key):
            return key.content_length

        def get_name(self, obj):
            return obj.key

        def get_as_string(self, keyname):
            return self.get_key(keyname).get()['Body'].read()

        def store_file_to_keyname(self, keyname, path):
            k = self.get_key(keyname)
            k.put(Body=open(path, 'rb'))
            return k

        def fetch_file(self, key, filename, path):
            if isinstance(key, (str, unicode)):
                key = self.get_key(key)

            if not self.exists_key(key):
                self.l.error("Key has dissapeared out of S3 bucket! %s", filename)
            else:
                with open(path, 'wb') as output:
                    shutil.copyfileobj(key.get()['Body'], output)
                return key

    def get_helper(*args, **kwargs):
        return Boto3Helper(*args, **kwargs)

except ImportError:
    from boto.s3.key import Key
    from boto.s3.connection import S3Connection

    class BotoHelper(S3Helper):
        @property
        def s3(self):
            return S3Connection(aws_access_key, aws_secret_key)

        @property
        def bucket(self):
            if self.b is None:
                self.b = self.s3.get_bucket(s3_bucket_name)
            return self.b

        def list_keys(self):
            return self.bucket.list()

        def key_names(self):
            return [key.name for key in self.list_keys()]

        def exists_key(self, key):
            return self.bucket.get_key(key) is not None

        def get_key(self, keyname):
            return self.bucket.get_key(keyname)

        def get_etag(self, key):
            return key.etag

        def get_size(self, key):
            return key.size

        def get_name(self, key):
            return key.name

        def get_as_string(self, keyname):
            return self.get_key(keyname).get_contents_as_string()

        def store_file_to_keyname(self, keyname, path):
            k = Key(self.bucket)
            k.key = keyname
            k.set_contents_from_filename(filename)

            return k

        def fetch_file(self, key, filename, path):
            if isinstance(key, (str, unicode)):
                key = self.bucket.get_key(key)

            if key is None:
                self.l.error("Key has dissapeared out of S3 bucket! %s", filename)
            else:
                self.key.get_contents_to_filename(path)
                return key

    def get_helper(*args, **kwargs):
        return BotoHelper(*args, **kwargs)
