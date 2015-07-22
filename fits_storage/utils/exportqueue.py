"""
This module provides various utility function used to manage the export queue
"""
import os
import urllib2
import json
import datetime
import hashlib
import bz2

from sqlalchemy import desc, join
from sqlalchemy.orm import make_transient
from sqlalchemy.orm.exc import ObjectDeletedError

from ..fits_storage_config import storage_root, using_s3, export_bzip
from ..logger import logger

from ..orm.file import File
from ..orm.diskfile import DiskFile
from ..orm.exportqueue import ExportQueue

import ..apache_return_codes as apache

if using_s3:
    from boto.s3.connection import S3Connection
    from ..fits_storage_config import aws_access_key, aws_secret_key, s3_bucket_name
    import logging
    logging.getLogger('boto').setLevel(logging.CRITICAL)

def add_to_exportqueue(session, filename, path, destination):
    """
    Adds a file to the export queue
    """
    logger.info("Adding file %s to %s to exportqueue", filename, destination)
    eq = ExportQueue(filename, path, destination)
    logger.debug("Instantiated ExportQueue object")
    session.add(eq)
    session.commit()
    logger.debug("Added to database")
    try:
        logger.debug("Added id %d for filename %s to exportqueue", eq.id, eq.filename)
        return eq.id
    except ObjectDeletedError:
        logger.debug("Added filename %s to exportqueue which was immediately deleted", filename)


def export_file(session, filename, path, destination):
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
    filename_nobz2 = filename
    if filename_nobz2.endswith('.bz2'):
        filename_nobz2 = filename_nobz2[:-4]

    # Search Database
    query = session.query(DiskFile).select_from(join(File, DiskFile))
    query = query.filter(DiskFile.present == True).filter(File.name == filename_nobz2)
    diskfile = query.one()
    our_md5 = diskfile.data_md5

    logger.debug("Checking for remote file md5")
    dest_md5 = get_destination_data_md5(filename, destination)

    if (dest_md5 is not None) and (dest_md5 == our_md5):
        logger.info("Data %s is already at %s with md5 %s", filename, destination, dest_md5)
        return True
    logger.debug("Data not present at destination: dest_md5: %s, our_md5: %s - reading file", dest_md5, our_md5)

    # Read the file into the payload postdata buffer to HTTP POST
    if using_s3:
        # Read the file from S3
        s3conn = S3Connection(aws_access_key, aws_secret_key)
        bucket = s3conn.get_bucket(s3_bucket_name)
        key = bucket.get_key(os.path.join(path, filename))
        if key is None:
            logger.error("cannot access %s in S3 bucket", filename)
            data = None
        else:
            data = key.get_contents_as_string()
    else:
        # Read the file from disk
        fullpath = os.path.join(storage_root, path, filename)
        exists = os.access(fullpath, os.F_OK | os.R_OK) and os.path.isfile(fullpath)
        if not exists:
            logger.error("cannot access %s", fullpath)
            data = None
        else:
            f = open(fullpath, 'r')
            data = f.read()
            f.close()

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
        filename = filename[:-4]
        our_md5 = diskfile.data_md5

    # Construct upload URL
    url = "http://%s/upload_file/%s" % (destination, filename)

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
        u = urllib2.urlopen(request)
        response = u.read()
        u.close()
        http_status = u.getcode()
        logger.debug("Got status code: %d and response: %s", http_status, response)

        # verify that it transfered OK
        ok = True
        if http_status == apache.HTTP_OK:
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
            logger.debug("Transfer not sucesfull")
            return False

    except urllib2.URLError:
        logger.info("URLError posting %d bytes of data to destination server at: %s", len(postdata), url)
        logger.debug("Transfer Failed")
        return False
    except:
        logger.error("Problem posting %d bytes of data to destination server at: %s", len(postdata), url)
        raise

def pop_exportqueue(session):
    """
    Returns the next thing to export off the export queue, and sets the
    inprogress flag on that entry.

    The select and update inprogress are done with a transaction lock
    to avoid race conditions or duplications when there is more than
    one process processing the export queue.

    Next to export is defined by a sort on the filename to get the
    newest filename that is not already inprogress.

    Also, when we go inprogress on an entry in the queue, we
    delete all other entries for the same filename.

    The instance returned is actually a transient instance  - it is
    not associated with the session. Basically treat it as a convenience
    container (like a dictionary) for the values therein, but don't try
    to modify the database using it.
    """

    # This is strongly based on pop_ingestqueue, but they're sufficiently
    # different it would be annoying to make a common function

    session.execute("LOCK TABLE exportqueue IN ACCESS EXCLUSIVE MODE;")
    query = session.query(ExportQueue).filter(ExportQueue.inprogress == False).order_by(desc(ExportQueue.filename))

    eq = query.first()
    if eq == None:
        logger.debug("No item to pop on exportqueue")
    else:
        # OK, we got a viable item, set it to inprogress and return it.
        logger.debug("Popped id %d from exportqueue", eq.id)
        # Set this entry to in progres and flush to the DB.
        eq.inprogress = True
        session.flush()

        # Find other instances and delete them
        query = session.query(ExportQueue)
        query = query.filter(ExportQueue.inprogress == False).filter(ExportQueue.filename == eq.filename)
        query.delete()

        # Make the eq into a transient instance before we return it
        # This detaches it from the session, basically it becomes a convenience container for the
        # values (filename, path, etc). The problem is that if it's still attached to the session
        # but expired (because we did a commit) then the next reference to it will initiate a transaction
        # and a SELECT to refresh the values, and that transaction will then hold a FOR ACCESS SHARE lock
        # on the exportqueue table until we complete the export and do a commit - which will prevent
        # the ACCESS EXCLUSIVE lock in pop_exportqueue from being granted until the transfer completes.
        make_transient(eq)

    # And we're done, commit the transaction and release the update lock
    session.commit()

    return eq


def exportqueue_length(session):
    """
    return the length of the export queue
    """
    # Make this generic between the ingest and export queues
    length = session.query(ExportQueue).filter(ExportQueue.inprogress == False).count()
    # Even though there's nothing to commit, close the transaction
    session.commit()
    return length

def get_destination_data_md5(filename, destination):
    """
    Queries the jsonfilelist url at the destination to get the md5 of the file
    at the destination.
    """

    # Construct and retrieve the URL
    try:
        url = "http://%s/jsonfilelist/present/filename=%s" % (destination, filename)
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

def retry_failures(session, interval):
    """
    This function looks for failed transfers in the export queue, where
    the failure is more than <interval> ago, and flags them for re-try
    """

    before = datetime.datetime.now()
    before -= interval

    query = session.query(ExportQueue).filter(ExportQueue.inprogress == True)
    query = query.filter(ExportQueue.lastfailed < before)

    num = query.update({"inprogress": False})
    logger.info("There are %d failed ExportQueue items to retry", num)

    session.commit()
