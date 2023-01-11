"""
This module provides various utility function used to manage the export queue
"""
import os

import requests
import json
import datetime
import hashlib
import bz2
import http.client
import ssl
from requests import RequestException

from sqlalchemy import join
from sqlalchemy.orm import make_transient
from sqlalchemy.exc import IntegrityError

from ..fits_storage_config import storage_root, using_s3, export_bzip, export_upload_auth_cookie, z_staging_area, \
    get_export_upload_auth_cookie
from . import queue

from gemini_obs_db.orm.file import File
from gemini_obs_db.orm.diskfile import DiskFile
from ..orm.exportqueue import ExportQueue

from .. import apache_return_codes as apache

if using_s3:
    from .aws_s3 import get_helper


class ExportQueueUtil(object):
    """
    Utility for working with the :class:`~orm.exportqueue.ExportQueue`
    """
    def __init__(self, session, logger):
        """
        Create an :class:`~ExportQueueUtil` utility

        Parameters
        ----------
        session : :class:`sqlalchemy.orm.session.Session`
            SQL Alchemy session to use
        logger : :class:`logger.Logger`
            Logger to use
        """
        self.s = session
        self.l = logger

    def length(self):
        """
        Get the length of the export queue

        Returns
        -------
        int : length of queue
        """
        return queue.queue_length(ExportQueue, self.s)

    def pop(self):
        """
        Get the next record off the queue

        Returns
        -------
        :class:`~ExportQueue` : next export queue item
        """
        return queue.pop_queue(ExportQueue, self.s, self.l, fast_rebuild=False)

    def set_error(self, trans, exc_type, exc_value, tb):
        """
        Sets an error message to a transient object
        """
        queue.add_error(ExportQueue, trans, exc_type, exc_value, tb, self.s)

    def delete(self, trans):
        """
        Deletes a transient object
        """
        queue.delete_with_id(ExportQueue, trans.id, self.s)

    def set_deferred(self, trans):
        """
        Set export to deferred.

        This is used when an export is pending ingest on the target host.  In this case,
        we use the `after` field to defer the ingest for 10 seconds.  We don't want to wait
        the full failure timeout which defaults to 5 minutes.

        Parameters
        ----------
        trans : :class:`~fits_storage.orm.exportqueue.ExportQueue`
            queue item to set set `after` field for
        """
        self.s.query(ExportQueue).get(trans.id).after = datetime.datetime.now() + datetime.timedelta(seconds=10)
        self.s.query(ExportQueue).get(trans.id).inprogress = False
        self.s.commit()

    def set_last_failed(self, trans):
        """
        Set export to failed and update last failed timestamp.

        Parameters
        ----------
        trans : :class:`~fits_storage.orm.exportqueue.ExportQueue`
            queue item to set failed timestamp and flag on
        """
        self.s.query(ExportQueue).get(trans.id).lastfailed = datetime.datetime.now()
        self.s.query(ExportQueue).get(trans.id).failed = True
        self.s.commit()

    def add_to_queue(self, filename, path, destination):
        """
        Adds a file to the export queue.

        Upon success, returns a transient object representing the queue entry. Otherwise,
        it returns None.

        Parameters
        ----------
        filename : str
            Name of file to add to queue
        path : str
            Path of file within the storage root
        destination : str
            URL of destination service to export to

        Returns
        -------
        :class:`~fits_storage.orm.ExportQueue`
            queue item if successful
        """
        self.l.info(f"Adding file {filename} to {destination} to exportqueue")

        # Trying without this, seems it is causing a race condition where a file is updated during an existing export
        # query = self.s.query(ExportQueue)\
        #             .filter(ExportQueue.filename == filename)\
        #             .filter(ExportQueue.path == path)\
        #             .filter(ExportQueue.destination == destination)
        # check_export = query.one_or_none()
        # if check_export is not None:
        #     self.l.info(f"Already have entry to export file {filename} to {destination}, ignoring")
        #     return check_export

        eq = ExportQueue(filename, path, destination)
        self.l.debug("Instantiated ExportQueue object")
        self.s.add(eq)

        try:
            self.s.commit()
        except IntegrityError:
            # table is not sufficiently constrained
            self.l.debug(f"File {eq.filename} seems to be in the queue")
            self.s.rollback()
        else:
            make_transient(eq)
            self.l.debug(f"Added id {eq.id} for filename {eq.filename} to exportqueue")
            return eq

    def export_file(self, filename, path, destination):
        """
        Exports a file to a downstream server.

        Parameters
        ----------
        filename : str
            Name of the file to export
        path : str
            Path in `dataflow` of the file
        destination : str
            URL of the service to export to

        Returns
        -------
        bool, str : True if sucessful, False failed with a str reason
        """
        self.l.debug(f"export_file {filename} to {destination}")

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
        try:
            query = self.s.query(DiskFile).select_from(join(File, DiskFile))\
                        .filter(DiskFile.present == True)\
                        .filter(File.name == filename_nobz2)
            diskfile = query.one()
        except:
            self.l.error("Could not find present diskfile for File entry with name %s", filename_nobz2)
            return False, "not found"
        our_md5 = diskfile.data_md5

        self.l.debug("Checking for remote file md5")
        dest_md5, pending_ingest = get_destination_data_md5(filename_nobz2, self.l, destination)

        if dest_md5 == 'ERROR':
            return False, "md5 error"

        if pending_ingest:
            # we need to wait, relies on failure requeue
            self.l.warning(f"File {filename} queued for ingest on destination, failing export to requeue later")
            return False, "pending ingest"

        if (dest_md5 is not None) and (dest_md5 == our_md5):
            self.l.info(f"Data {filename} is already at {destination} with md5 {dest_md5}")
            return True, ""
        self.l.debug(f"Data not present at destination: dest_md5: {dest_md5}, our_md5: {our_md5} - reading file")

        # Read the file into the payload postdata buffer to HTTP POST
        data = None
        if using_s3:
            # Read the file from S3
            keyname = os.path.join(path, filename)
            s3 = get_helper()
            if not s3.exists_key(keyname):
                self.l.error(f"cannot access {filename} in S3 bucket")
            else:
                data = s3.get_as_string(keyname).get_contents_as_string()
        else:
            # Read the file from disk
            fullpath = os.path.join(storage_root, path, filename)
            try:
                data = open(fullpath, 'rb').read()
            except IOError:
                self.l.error(f"cannot access {fullpath}")

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
            self.l.info(f"Transferring file {filename} to destination {destination}")
            postdata = bytearray(data)
            data = None

            if len(postdata) < 2147483647:
                cookies = {'gemini_fits_upload_auth': get_export_upload_auth_cookie(destination)}
                r = requests.post(url, data=postdata, cookies=cookies, timeout=600)
                response = r.text
                http_status = r.status_code
            else:
                tmpfilename = os.path.join(z_staging_area, filename)

                with open(tmpfilename, 'wb') as tmpfile:
                    tmpfile.write(postdata)
                    tmpfile.close()

                with open(tmpfilename, 'rb') as tmpfile:
                    self.l.info(f"Large file {filename} in export, using alternate method to send the data")
                    headers = {'Cache-Control': 'no-cache', 'Content-Length': '%d' % len(postdata)}
                    cookies = {'gemini_fits_upload_auth': get_export_upload_auth_cookie(destination)}
                    r = requests.post(url, headers=headers, cookies=cookies, data=tmpfile, timeout=600)
                    response = r.text
                    http_status = r.status_code
                os.unlink(tmpfilename)

            self.l.debug(f"Got status code: {http_status} and response: {response}")

            # verify that it transfered OK
            ok = True
            if http_status == apache.OK:
                # response is a short json document
                verification = json.loads(response)[0]
                if verification['filename'] != filename:
                    self.l.error("Transfer Verification Filename mismatch: %s vs %s" %
                                 (verification['filename'], filename))
                    ok = False
                if verification['size'] != len(postdata):
                    self.l.error("Transfer Verification size mismatch: %s vs %s" %
                                 (verification['size'], len(postdata)))
                    ok = False
                if verification['md5'] != our_md5:
                    self.l.error("Transfer Verification md5 mismatch: %s vs %s" % (verification['md5'], our_md5))
                    ok = False
            else:
                self.l.error("Bad HTTP status code transferring %s to %s" % (filename, destination))
                ok = False

            if ok:
                self.l.debug("Transfer sucessfull")
                return True, ""
            else:
                self.l.debug("Transfer not successful")
                return False, "transfer failed"

        except (RequestException, http.client.IncompleteRead, ssl.SSLError):
            self.l.info("Error posting %d bytes of data to destination server at: %s" % (len(postdata), url))
            self.l.debug("Transfer Failed")
            return False, "transfer connection error"
        except:
            self.l.error("Problem posting of data to destination server at: %s" % url)
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
            self.l.info("There are %d failed ExportQueue items to retry" % num)
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
        r = requests.get(url, timeout=10)
        json_data = r.text
    except (RequestException, http.client.IncompleteRead):
        logger.debug("Failed to get json data from destination server at URL: %s" % url)
        return "ERROR", False

    try:
        thelist = json.loads(json_data)
    except ValueError:
        logger.debug("JSON decode failed. JSON data: %s" % json_data)
        return "ERROR", False

    if len(thelist) == 0:
        logger.debug("Destination server does not have filename %s" % filename)
        return None, False
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
            return thedict['data_md5'], thedict.get("pending_ingest", False)

    return "ERROR", False
