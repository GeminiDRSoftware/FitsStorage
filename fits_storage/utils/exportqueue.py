"""
This module provides various utility function used to manage the export queue
"""
import os
import urllib2
import json
import datetime
import hashlib
import bz2
import functools

from sqlalchemy import desc, join
from sqlalchemy.orm import make_transient
from sqlalchemy.orm.exc import ObjectDeletedError

from ..fits_storage_config import storage_root, using_s3, export_bzip, upload_auth_cookie
from . import queue

from ..orm.file import File
from ..orm.diskfile import DiskFile
from ..orm.exportqueue import ExportQueue

from .. import apache_return_codes as apache

if using_s3:
    from boto.s3.connection import S3Connection
    from ..fits_storage_config import aws_access_key, aws_secret_key, s3_bucket_name
    import logging
    logging.getLogger('boto').setLevel(logging.CRITICAL)

def add_to_exportqueue(session, logger, filename, path, destination):
    """
    Adds a file to the export queue
    """
    logger.info("Adding file %s to %s to exportqueue", filename, destination)
    eq = ExportQueue(filename, path, destination)
    logger.debug("Instantiated ExportQueue object")
    session.add(eq)
    session.commit()
    make_transient(eq)
    logger.debug("Added id %d for filename %s to exportqueue", eq.id, eq.filename)


def export_file(session, logger, filename, path, destination):
    """
    Exports a file to a downstream server.

    Returns True if sucessfull, False otherwise
    """
    logger.debug("export_file %s to %s", filename, destination)

    # First, lookup the md5 of the file we have, and see if the
    # destination server already has it with that md5
    # This is all done with the data md5 not the file, if the correct data
    # is there at the other end but the compression state is wrong, we're not going
    # to re-export it here.
    # To ignore the compression factor, we match against File.name rather than DiskFile.filename
    # and we strip any .bz2 from the local filename

    # Strip any .bz2 from local filename
    filename_nobz2 = File.trim_name(filename)

    # Search Database
    query = session.query(DiskFile).select_from(join(File, DiskFile))\
                .filter(DiskFile.present == True)\
                .filter(File.name == filename_nobz2)
    diskfile = query.one()
    our_md5 = diskfile.data_md5

    logger.debug("Checking for remote file md5")
    dest_md5 = get_destination_data_md5(filename, logger, destination)

    if (dest_md5 is not None) and (dest_md5 == our_md5):
        logger.info("Data %s is already at %s with md5 %s", filename, destination, dest_md5)
        return True
    logger.debug("Data not present at destination: dest_md5: %s, our_md5: %s - reading file", dest_md5, our_md5)

    # Read the file into the payload postdata buffer to HTTP POST
    data = None
    if using_s3:
        # Read the file from S3
        s3conn = S3Connection(aws_access_key, aws_secret_key)
        bucket = s3conn.get_bucket(s3_bucket_name)
        key = bucket.get_key(os.path.join(path, filename))
        if key is None:
            logger.error("cannot access %s in S3 bucket", filename)
        else:
            data = key.get_contents_as_string()
    else:
        # Read the file from disk
        fullpath = os.path.join(storage_root, path, filename)
        try:
            data = open(fullpath, 'r').read()
        except IOError:
            logger.error("cannot access %s", fullpath)

    # Do we need to compress or uncompress the data?
    # If the data are already compressed, we're not going to re-compress it
    # And don't try to pass a unicode filename.
    filename = filename.encode('ascii', 'ignore')
    if export_bzip and diskfile.compressed == False:
        # Need to compress it
        logger.debug("bzip2ing file on the fly")
        data = bz2.compress(data)
        # Add .bz2 to the filename from here on, update our_md5
        filename += '.bz2'
        m = hashlib.md5()
        m.update(data)
        our_md5 = m.hexdigest()

    if (export_bzip is None) and (diskfile.compressed == True):
        # Need to uncompress it
        logger.debug("gunzipping on the fly")
        data = bz2.decompress(data)
        # Trim .bz2 from the filename from here on, update our_md5
        filename = File.trime_name(filename)
        our_md5 = diskfile.data_md5

    # Construct upload URL
    url = "%s/upload_file/%s" % (destination, filename)

    # Connect to the URL and post the data
    # NB need to make the data buffer into a bytearray not a str
    # Otherwise get ascii encoding errors from httplib layer
    try:
        logger.info("Transferring file %s to destination %s", filename, destination)
        postdata = bytearray(data)
        data = None
        request = urllib2.Request(url, data=postdata)
        request.add_header('Cache-Control', 'no-cache')
        request.add_header('Content-Length', '%d' % len(postdata))
        request.add_header('Content-Type', 'application/octet-stream')
        request.add_header('Cookie', 'gemini_fits_upload_auth=%s' % upload_auth_cookie)
        u = urllib2.urlopen(request)
        response = u.read()
        u.close()
        http_status = u.getcode()
        logger.debug("Got status code: %d and response: %s", http_status, response)

        # verify that it transfered OK
        ok = True
        if http_status == apache.OK:
            # response is a short json document
            verification = json.loads(response)[0]
            if verification['filename'] != filename:
                logger.error("Transfer Verification Filename mismatch: %s vs %s", verification['filename'], filename)
                ok = False
            if verification['size'] != len(postdata):
                logger.error("Transfer Verification size mismatch: %s vs %s", verification['size'], len(postdata))
                ok = False
            if verification['md5'] != our_md5:
                logger.error("Transfer Verification md5 mismatch: %s vs %s", verification['md5'], our_md5)
                ok = False
        else:
            logger.error("Bad HTTP status code transferring %s to %s", filename, destination)
            ok = False

        if ok:
            logger.debug("Transfer sucessfull")
            return True
        else:
            logger.debug("Transfer not successful")
            return False

    except urllib2.URLError:
        logger.info("URLError posting %d bytes of data to destination server at: %s", len(postdata), url)
        logger.debug("Transfer Failed")
        return False
    except:
        logger.error("Problem posting %d bytes of data to destination server at: %s", len(postdata), url)
        raise

pop_exportqueue = functools.partial(queue.pop_queue, ExportQueue, fast_rebuild = False)
exportqueue_length = functools.partial(queue.queue_length, ExportQueue)

def get_destination_data_md5(filename, logger, destination):
    """
    Queries the jsonfilelist url at the destination to get the md5 of the file
    at the destination.
    """

    # Construct and retrieve the URL
    try:
        url = "%s/jsonfilelist/present/filename=%s" % (destination, filename)
        u = urllib2.urlopen(url)
        json_data = u.read()
        u.close()
    except urllib2.URLError:
        logger.error("Failed to get json data from destination server at URL: %s", url)

    try:
        thelist = json.loads(json_data)
    except ValueError:
        logger.error("JSON decode failed. JSON data: %s", json_data)

    if len(thelist) == 0:
        logger.debug("Destination server does not have filename %s", filename)
        return None
    if len(thelist) > 1:
        logger.error("Got multiple results from destination server")
    else:
        thedict = thelist[0]
        if 'filename' not in thedict.keys():
            logger.error("No filename in json data")
        elif thedict['filename'] not in [filename, filename+'.bz2']:
            logger.error("Wrong filename in json data")
        elif 'data_md5' not in thedict.keys():
            logger.error("No data_md5 in json data")
        else:
            return thedict['data_md5']

    # Should never get here
    return None

def retry_failures(session, logger, interval):
    """
    This function looks for failed transfers in the export queue, where
    the failure is more than <interval> ago, and flags them for re-try
    """

    before = datetime.datetime.now()
    before -= interval

    query = session.query(ExportQueue)\
                .filter(ExportQueue.inprogress == True)\
                .filter(ExportQueue.lastfailed < before)

    num = query.update({"inprogress": False})
    if num > 0:
        logger.info("There are %d failed ExportQueue items to retry", num)
    else:
        logger.debug("There are %d failed ExportQueue items to retry", num)

    session.commit()
