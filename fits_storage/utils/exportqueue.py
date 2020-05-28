"""
This module provides various utility function used to manage the export queue
"""
import os
import urllib.request, urllib.error, urllib.parse
import json
import datetime
import hashlib
import bz2
import functools
import http.client
import ssl

from sqlalchemy import desc, join
from sqlalchemy.orm import make_transient
from sqlalchemy.orm.exc import ObjectDeletedError
from sqlalchemy.exc import IntegrityError

from ..fits_storage_config import storage_root, using_s3, export_bzip, export_upload_auth_cookie
from . import queue

from ..orm.file import File
from ..orm.diskfile import DiskFile
from ..orm.exportqueue import ExportQueue

from .. import apache_return_codes as apache

if using_s3:
    from .aws_s3 import get_helper

class ExportQueueUtil(object):
    def __init__(self, session, logger):
        self.s = session
        self.l = logger

    def length(self):
        return queue.queue_length(ExportQueue, self.s)

    def pop(self):
        return queue.pop_queue(ExportQueue, self.s, self.l, fast_rebuild=False)

    def set_error(self, trans, exc_type, exc_value, tb):
        "Sets an error message to a transient object"
        queue.add_error(ExportQueue, trans, exc_type, exc_value, tb, self.s)

    def delete(self, trans):
        "Deletes a transient object"
        queue.delete_with_id(ExportQueue, trans.id, self.s)

    def set_last_failed(self, trans):
        self.s.query(ExportQueue).get(trans.id).lastfailed = datetime.datetime.now()
        self.s.commit()

    def add_to_queue(self, filename, path, destination):
        """
        Adds a file to the export queue.

        Upon success, returns a transient object representing the queue entry. Otherwise,
        it returns None.
        """
        self.l.info("Adding file %s to %s to exportqueue", filename, destination)

        query = self.s.query(ExportQueue)\
                    .filter(ExportQueue.filename == filename)\
                    .filter(ExportQueue.path == path)\
                    .filter(ExportQueue.destination == destination)
        check_export = query.one_or_none()
        if check_export is not None:
            self.l.info("Already have entry to export file %s to %s, ignoring", filename, destination)
            return check_export

        eq = ExportQueue(filename, path, destination)
        self.l.debug("Instantiated ExportQueue object")
        self.s.add(eq)

        try:
            self.s.commit()
        except IntegrityError:
            # table is not sufficiently constrained
            self.l.debug("File %s seems to be in the queue", eq.filename)
            self.s.rollback()
        else:
            make_transient(eq)
            self.l.debug("Added id %s for filename %s to exportqueue", eq.id, eq.filename)
            return eq

    def export_file(self, filename, path, destination):
        """
        Exports a file to a downstream server.

        Returns True if sucessfull, False otherwise
        """
        self.l.debug("export_file %s to %s", filename, destination)

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
        query = self.s.query(DiskFile).select_from(join(File, DiskFile))\
                    .filter(DiskFile.present == True)\
                    .filter(File.name == filename_nobz2)
        diskfile = query.one()
        our_md5 = diskfile.data_md5

        self.l.debug("Checking for remote file md5")
        dest_md5 = get_destination_data_md5(filename, self.l, destination)

        if dest_md5 == 'ERROR':
            return False

        if (dest_md5 is not None) and (dest_md5 == our_md5):
            self.l.info("Data %s is already at %s with md5 %s", filename, destination, dest_md5)
            return True
        self.l.debug("Data not present at destination: dest_md5: %s, our_md5: %s - reading file", dest_md5, our_md5)

        # Read the file into the payload postdata buffer to HTTP POST
        data = None
        if using_s3:
            # Read the file from S3
            keyname = os.path.join(path, filename)
            s3 = get_helper()
            if not s3.exists_key(keyname):
                self.l.error("cannot access %s in S3 bucket", filename)
            else:
                data = s3.get_as_string(keyname).get_contents_as_string()
        else:
            # Read the file from disk
            fullpath = os.path.join(storage_root, path, filename)
            try:
                data = open(fullpath, 'rb').read()
            except IOError:
                self.l.error("cannot access %s", fullpath)

        # Do we need to compress or uncompress the data?
        # If the data are already compressed, we're not going to re-compress it
        # And don't try to pass a unicode filename.
        filename = filename.encode('ascii', 'ignore').decode('ascii')
        if export_bzip and diskfile.compressed == False:
            # Need to compress it
            self.l.debug("bzip2ing file on the fly")
            data = bz2.compress(data)
            # Add .bz2 to the filename from here on, update our_md5
            filename += '.bz2'
            m = hashlib.md5()
            m.update(data)
            our_md5 = m.hexdigest()

        if (export_bzip is None) and (diskfile.compressed == True):
            # Need to uncompress it
            self.l.debug("gunzipping on the fly")
            data = bz2.decompress(data)
            # Trim .bz2 from the filename from here on, update our_md5
            filename = File.trim_name(filename)
            our_md5 = diskfile.data_md5
        if export_bzip and diskfile.compressed:
            # All good to go, just need to get the right md5 for the transfer verification
            our_md5 = diskfile.file_md5

        # Construct upload URL
        url = "%s/upload_file/%s" % (destination, filename)

        # Connect to the URL and post the data
        # NB need to make the data buffer into a bytearray not a str
        # Otherwise get ascii encoding errors from httplib layer
        try:
            self.l.info("Transferring file %s to destination %s", filename, destination)
            postdata = bytearray(data)
            data = None
            request = urllib.request.Request(url, data=postdata)
            request.add_header('Cache-Control', 'no-cache')
            request.add_header('Content-Length', '%d' % len(postdata))
            request.add_header('Content-Type', 'application/octet-stream')
            request.add_header('Cookie', 'gemini_fits_upload_auth=%s' % export_upload_auth_cookie)
            u = urllib.request.urlopen(request, timeout=120)
            response = u.read()
            u.close()
            http_status = u.getcode()
            self.l.debug("Got status code: %d and response: %s", http_status, response)

            # verify that it transfered OK
            ok = True
            if http_status == apache.OK:
                # response is a short json document
                verification = json.loads(response)[0]
                if verification['filename'] != filename:
                    self.l.error("Transfer Verification Filename mismatch: %s vs %s", verification['filename'], filename)
                    ok = False
                if verification['size'] != len(postdata):
                    self.l.error("Transfer Verification size mismatch: %s vs %s", verification['size'], len(postdata))
                    ok = False
                if verification['md5'] != our_md5:
                    self.l.error("Transfer Verification md5 mismatch: %s vs %s", verification['md5'], our_md5)
                    ok = False
            else:
                self.l.error("Bad HTTP status code transferring %s to %s", filename, destination)
                ok = False

            if ok:
                self.l.debug("Transfer sucessfull")
                return True
            else:
                self.l.debug("Transfer not successful")
                return False

        except (urllib.error.URLError, http.client.IncompleteRead, ssl.SSLError):
            self.l.info("Error posting %d bytes of data to destination server at: %s", len(postdata), url)
            self.l.debug("Transfer Failed")
            return False
        except:
            self.l.error("Problem posting %d bytes of data to destination server at: %s", len(postdata), url)
            raise

    def retry_failures(self, interval):
        """
        This function looks for failed transfers in the export queue, where
        the failure is more than <interval> ago, and flags them for re-try
        """

        before = datetime.datetime.now()
        before -= interval

        query = self.s.query(ExportQueue)\
                    .filter(ExportQueue.inprogress == True)\
                    .filter(ExportQueue.lastfailed < before)

        num = query.update({"inprogress": False})
        if num > 0:
            self.l.info("There are %d failed ExportQueue items to retry", num)
        else:
            self.l.debug("There are no failed ExportQueue items to retry")

        self.s.commit()

def get_destination_data_md5(filename, logger, destination):
    """
    Queries the jsonfilelist url at the destination to get the md5 of the file
    at the destination.
    Return None if destination does not have the file
    Return md5sum if it does
    Return "ERROR" if an error occurred
    """

    # Construct and retrieve the URL
    try:
        url = "%s/jsonfilelist/present/filename=%s" % (destination, filename)
        u = urllib.request.urlopen(url)
        json_data = u.read()
        u.close()
    except (urllib.error.URLError, http.client.IncompleteRead):
        logger.debug("Failed to get json data from destination server at URL: %s", url)
        return "ERROR"

    try:
        thelist = json.loads(json_data)
    except ValueError:
        logger.debug("JSON decode failed. JSON data: %s", json_data)
        return "ERROR"

    if len(thelist) == 0:
        logger.debug("Destination server does not have filename %s", filename)
        return None
    if len(thelist) > 1:
        logger.debug("Got multiple results from destination server")
    else:
        thedict = thelist[0]
        if 'filename' not in list(thedict.keys()):
            logger.debug("No filename in json data")
        elif thedict['filename'] not in [filename, filename+'.bz2']:
            logger.debug("Wrong filename in json data")
        elif 'data_md5' not in list(thedict.keys()):
            logger.debug("No data_md5 in json data")
        else:
            return thedict['data_md5']

    return "ERROR"
