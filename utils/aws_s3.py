"""
This module contains utility functions for interacting with AWS S3
"""

import os
import sys
import traceback
from time import sleep

from fits_storage_config import using_s3, storage_root
from logger import logger
from utils.hashes import md5sum

if(using_s3):
    from boto.s3.connection import S3Connection
    from fits_storage_config import aws_access_key, aws_secret_key, s3_bucket_name


def get_s3_md5(key):
    """
    Get the MD5 that the S3 server hs for this key.
    Simply strips quotes from the etag value.
    """
    return key.etag.replace('"', '')


def fetch_to_staging(path, filename, key=None, fullpath=None):
    """
    Fetch the file from s3 and put it in the storage_root directory.
    Do some validation, and re-try as appropriate
    Return True if suceeded, False otherwise
    """

    # Make the full path of the destination file if we were not given one
    if(fullpath is None):
        fullpath = os.path.join(storage_root, filename)

    # Check if the file already exists in the staging area, remove it if so
    if(os.path.exists(fullpath)):
        logger.warning("File already exists at S3 download location: %s. Will delete it first."  % fullpath)
        try:
            os.unlink(fullpath)
        except:
            logger.error("Unable to delete %s which is in the way of the S3 download" % fullpath)

    # Try up to 5 times. Have seen socket.error raised 
    tries = 0
    gotit = False
    while((not gotit) and (tries < 5)):
        tries += 1
        logger.debug("Fetching %s to s3_staging_area, try %d" % (filename, tries))

        # If we do not have a key object, get one
        if(key is None):
            key = bucket.get_key(os.path.join(path, filename))

        try:
            if(key is None):
                logger.error("Key has dissapeared out of S3 bucket! %s", filename)
            else:
                key.get_contents_to_filename(fullpath)
        except socket.error:
            # OK, we got a socket error.
            logger.debug("Socket Error fetching %s from S3 - will retry, tries=%d" % (filename, tries))
            logger.debug("Socket Error details: %s : %s... %s" % (sys.exc_info()[0], sys.exc_info()[1], traceback.format_tb(sys.exc_info()[2])))
            sleep(10)

            # Nullify the key object - seems like if it fails getting a new key is necessary
            key = None

            # Remove any partial file we got downloaded
            try:
                os.unlink(fullpath)
            except:
                pass

        # If we get here, it claimed to download ok
        gotit = True
        # Check size and md5
        filesize = os.path.getsize(fullpath)
        if(filesize != key.size):
            # Didn't get enough bytes
            gotit = False
            logger.error("Problem fetching %s from S3 - size mismatch - file: %s; key: %s" % (filename, filesize, key.size))
            sleep(10)
        filemd5 = md5sum(fullpath)
        if(gotit and (filemd5 != get_s3_md5(key))):
            # Size is OK, but md5 is not
            gotit = False
            logger.error("Problem fetching %s from S3 - size OK, but md5 mismatch - file: %s; key: %s" % (filename, filemd5, get_s3_md5(key)))
            sleep(10)

    if(gotit):
        logger.debug("Downloaded file from S3 sucessfully")
        return True
    else:
        logger.error("Failed to sucessfully download file %s from S3. Giving up." % filename)
        return False
