"""
This module contains code for exporting files
"""
import http

import requests
import requests.utils
import json
import datetime
import os
import time

from sqlalchemy.exc import MultipleResultsFound, NoResultFound

from fits_storage.queues.orm.exportqueueentry import ExportQueueEntry
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.server.bz2stream import StreamBz2Compressor
from fits_storage.core.hashes import md5sum

from fits_storage.config import get_config

import tempfile

class Exporter(object):
    """
    This class provides functionality for exporting files to another server.
    We instantiate this class in service_export_queue once, and feed it files
    to export with export_file().

    Note that this class does store per-file data in its data items, so in
    that sense it looks like you should instantiate it separately for each
    file, but that's not actually the case, it also stores per-session items
    such as a requests session to allow use of HTTP keep alive. There's a
    reset() method to call between individual files that resets the per-file
    data.

    """
    def __init__(self, session, logger, timeout=10):
        """
        Instantiate the Exporter class with a session, logger and any
        configuration items.

        Parameters
        ----------
        session - database session
        logger - Fits Storage Logger
        timeout = timeout value to pass to requests. Default 10s.
        """
        fsc = get_config()
        self.s = session
        self.l = logger
        self.timeout = timeout

        # Set up a requests session object with the magic cookie
        self.rs = requests.Session()
        cookie_dict = {'gemini_fits_upload_auth': fsc.export_auth_cookie}
        requests.utils.add_dict_to_cookiejar(self.rs.cookies, cookie_dict)

        # Note there is no way to set a default timeout value for the session,
        # that has to be set on each get or post call.

        # We pull these configuration values into the local namespace for
        # convenience and to allow poking them for testing
        self.storage_root = fsc.storage_root

        # got_destination_info tells us whether we have got the file info from
        # the destination server. None => Not attempted yet,
        # False => Failure getting the info, True => We got the info.
        # Note - can be True even if the file does not exist at the destination
        # in which case destination_md5 will be None.
        self.got_destination_info = None
        self.destination_md5 = None
        self.destination_ingest_pending = None

        # We store the exportqueueentry passed for export in the class for
        # convenience and to facilitate the seterror convenience function
        self.eqe = None

        # We store the diskfile we are exporting here too for convenience
        self.df = None

        # Reset / initialize per-file state
        self.reset()

    def reset(self):
        self.got_destination_info = None
        self.destination_md5 = None
        self.destination_ingest_pending = None
        self.eqe = None
        self.df = None

    def logeqeerror(self, message, exc_info=False):
        """
        Convenience function to log an error message,
        set the eqe error status, and commit the session
        """
        self.l.error(message, exc_info=exc_info)
        self.eqe.seterror(message)
        self.s.commit()

    def export_file(self, eqe: ExportQueueEntry):
        """
        Exports a file.
        """
        self.eqe = eqe
        self.l.debug("Export %s to %s" % (eqe.filename, eqe.destination))

        # First, get info about this file from the destination end, This info
        # is the data_md5 and also the ingest_pending flag
        self.got_destination_info = self._get_destination_file_info(eqe)

        if self.got_destination_info is False:
            error_text = "Failed to get destination file info for filename " \
                         f"{eqe.filename}. Marking transfer as failed."
            self.logeqeerror(error_text)
            return
        else:
            self.l.debug("Sucessfully got destination file info for %s",
                         eqe.filename)

        # if there is an ingest pending on this at the destination, we simply
        # log a message and postpone the export by 40 seconds
        if self.destination_ingest_pending:
            delay = 40
            self.l.info("File %s is ingest pending at destination %s. "
                        "Deferring ingest for %d seconds", eqe.filename,
                        eqe.destination, delay)
            now = datetime.datetime.utcnow()
            dt = datetime.timedelta(seconds=delay)
            eqe.after = now + dt
            eqe.inprogress = False
            self.s.commit()
            return

        # Look up the diskfile on the local server now
        if self.get_df() is False:
            # Could not look up the diskfile. get_df() will have already logged
            # the error, just bail out of the transfer.
            self.l.debug("get_df() returned False")
            return
        else:
            self.l.debug("get_df() good return")

        if self.at_destination():
            self.l.info("File %s is already at destination %s with correct "
                        "data_md5, skipping export",
                        eqe.filename, eqe.destination)
            self.s.delete(eqe)
            self.s.commit()
            return
        else:
            # at_destination returns False and sets eqe.failed = True if it
            # encountered an error. Check that here. It has already logged the
            # error message, so no need to repeat that here
            if eqe.failed:
                self.l.debug("exporter.at_destination() failed")
                return
            else:
                self.l.info("File %s is not present at destination %s with "
                            "correct data_md5.",
                            eqe.filename, eqe.destination)

        # If we get here, everything so far worked, and the file is not at the
        # destination with the correct data_md5. Go ahead and transfer it.
        self.l.debug("export_file: go ahead and transfer")
        # If we have header update data, we attempt to send the header update
        # to the destination and then verify we ended up with the same data_md5.
        # If we did, then the transfer is complete, if not, we fall back to a
        # regular file transfer.

        got_header_update = self.eqe.md5_before_header_update is not None\
                and self.eqe.md5_after_header_update is not None\
                and self.eqe.header_update is not None

        if got_header_update:
            self.l.info("Attempting pseudo-transfer by header update")
            if self.transfer_headers():
                self.l.info("Header update pseudo-transfer successful")
                self.s.delete(self.eqe)
                self.s.commit()
                return
            self.l.info("Header update failed. Falling back to file transfer")

        self.l.debug("export_file: calling file_transfer()")
        self.file_transfer()
        self.reset()
        return

    def transfer_headers(self):
        """
        Pseudo transfer the file to the destination using the header update
        hints. Return True on success, False on failure. If the before_md5
        does not match, immediately return False, otherwise attempt to apply
        the header_update and check the after_md5. Iff the after_md5 matches,
        return True.

        """
        self.l.error("transfer_headers not implemented yet")
        return False

    def file_transfer(self):
        """
        HTTP Post the bz2 compressed file to the destination.
        If successful, delete the eqe instance and commit the session.
        On failure, set status in the equ and commit the session.
        """
        fsc = get_config()
        # For convenience
        filename = self.eqe.filename
        path = self.eqe.path
        destination = self.eqe.destination

        self.l.info("Transferring file %s to destination %s",
                    filename, destination)
        if fsc.using_s3:
            self.l.error("Export from a server using_s3 is not implemented")
            return False

        # Get a file-like-object for the data to export
        # Note that we always export bz2 compressed data.
        fpfn = os.path.join(self.storage_root, path, filename)
        with open(fpfn, mode='rb') as f:
            if filename.endswith('.bz2'):
                destination_filename = filename
                flo = f
            else:
                destination_filename = filename + '.bz2'
                flo = StreamBz2Compressor(f)
            # Construct upload URL.
            url = "%s/upload_file/%s" % (destination, destination_filename)

            self.l.debug("POSTing data to %s", url)
            starttime = datetime.datetime.utcnow()
            try:
                req = self.rs.post(url, data=flo, timeout=self.timeout)
            except requests.Timeout:
                self.logeqeerror(f"Timeout posting {url}", exc_info=True)
                return
            except requests.ConnectionError:
                self.logeqeerror(f"ConnectionError posting {url}", exc_info=True)
                return
            except requests.RequestException:
                self.logeqeerror(f"RequestException posting {url}",
                                 exc_info=True)
                return
            enddtime = datetime.datetime.utcnow()

            secs = (enddtime - starttime).total_seconds()
            bytes_transferred = flo.bytes_output \
                if isinstance(flo, StreamBz2Compressor) else flo.tell()
            mbytes_transferred = bytes_transferred / 1048576

            self.l.info(f"Transfer completed: {mbytes_transferred:.2f} MB "
                        f"in {secs:.1f} secs - "
                        f"{mbytes_transferred/secs:.2f} MB/sec")

            self.l.debug("Got http status %s and response %s",
                         req.status_code, req.text)

            if req.status_code != http.HTTPStatus.OK:
                self.logeqeerror(f"Bad HTTP status: {req.status_code} from "
                                 f"upload post to url: {url}")
                # Insert a sleep here, to prevent rapid-fire failures in the
                # case where the server is in a bad state. This isn't ideal as
                # there may be exports to other destinations in the queue, and
                # it would be preferable to continue with those.
                # TODO - after we switch to sqlalchemy-2, change this to do a
                # TODO - bulk UPDATE on the exportqueue table to set after to
                # TODO - now()+30s where destination == self.destination.
                self.l.info("Waiting 30 seconds to prevent rapid-fire failures")
                time.sleep(30)
                return

            # The response should be a short json document
            if flo is f:
                # Need to provide the extra properties that our bz2streamer
                # provides that are used in the verification.
                flo.bytes_output = os.path.getsize(fpfn)
                flo.md5sum_output = md5sum(fpfn)
            try:
                verification = json.loads(req.text)[0]
                if verification['filename'] != destination_filename:
                    et = "Transfer Verification Filename mismatch: " \
                         f"{verification['filename']} vs {destination_filename}"
                    self.logeqeerror(et)
                    return
                if verification['size'] != flo.bytes_output:
                    et = "Transfer Verification size mismatch: " \
                         f"{verification['size']} vs {flo.bytes_output}"
                    self.logeqeerror(et)
                    return
                if verification['md5'] != flo.md5sum_output:
                    et = "Transfer Verification md5 mismatch: " \
                         f"{verification['md5']} vs {flo.md5sum_output}"
                    self.logeqeerror(et)
                    return

            except (TypeError, ValueError, KeyError):
                error_text = "Exception during transfer verification"
                self.l.error(error_text, exc_info=True)
                self.eqe.seterror(error_text)
                self.s.commit()

            self.l.debug("Transfer Verification succeeded")
            self.s.delete(self.eqe)
            self.s.commit()

    def get_df(self):
        """
        Find the diskfile we are exporting, and store it in the instance.
        Returns True on success, False (and logs the error to the logger and
        the eqe instance) on failure
        """
        # Note no need to worry about .bz2 here as by definition the file in
        # the export queue should match exactly what's in the database.
        query = self.s.query(DiskFile) \
            .filter(DiskFile.filename == self.eqe.filename) \
            .filter(DiskFile.path == self.eqe.path) \
            .filter(DiskFile.present == True)
        try:
            self.df = query.one()
        except MultipleResultsFound:
            self.l.debug("get_df MultipleResultsFound")
            et = "Multiple present diskfile entries found for filename " \
                 f"{self.eqe.filename}, path {self.eqe.path}. DATABASE IS " \
                 "CORRUPTED, Aborting Export and marking as failed"
            self.logeqeerror(et)
            return False
        except NoResultFound:
            self.l.debug("get_df NoResultFound")
            et = f"Cannot find present filename {self.eqe.filename}, path " \
                 f"{self.eqe.path} in diskfile table. Attempt to export a " \
                 "non-existent file. Marking export as failed."
            self.logeqeerror(et)
            return False
        return True

    def at_destination(self):
        """
        Return a boolean to say if the file in the export queue entry eqe
        is already at the destination server.
        If there are any errors, we return False and set the state in the
        eqe instance. If we return False, caller does need to check that to
        determine if there was an error.
        """

        if self.df is None:
            self.logeqeerror("Programming error calling at_destination() with"
                             "self.df is None in exporter.py")
            return False

        if self.df.data_md5 == self.destination_md5:
            self.l.debug("local and destination data_md5s match: %s",
                         self.df.data_md5)
            return True
        else:
            self.l.debug("local and destination data_md5s do not match: "
                         "diskfile id %d, data_md5 %s, destination data_md5 %s",
                         self.df.id, self.df.data_md5, self.destination_md5)
            return False

    def _get_destination_file_info(self, eqe):
        """
        Queries the jsonfilelist url at the destination to get the data_md5
        and ingest_pending value for the file at the destination. We strip any
        bz2 off the filename before querying.
        Populate self.destination_md5sum and self.destination_ingest_pending
        based in the jsonfilelist results.
        Return True if successfully got info, even if the file does not exist
        at the destination, or False if we have a failure of some kind
        while getting the info.
        """

        # Construct and retrieve the URL
        filename = eqe.filename.removesuffix('.bz2')
        url = "%s/jsonfilelist/present/filename=%s" % \
              (eqe.destination, filename)

        try:
            r = self.rs.get(url, timeout=self.timeout)
        except requests.Timeout:
            self.l.error("Timeout fetching %s", url)
            return False
        except requests.ConnectionError:
            self.l.error("ConnectionError fetching %s", url)
            return False
        except requests.RequestException:
            self.l.error("RequestException fetching %s", url, exc_info=True)
            return False

        json_text = r.text
        try:
            thelist = json.loads(json_text)
        except ValueError:
            self.l.error("JSON decode failed. JSON data: %s" % json_text,
                         exc_info=True)
            return False

        if len(thelist) == 0:
            self.l.debug("Destination server does not have filename %s",
                         filename)
            return True

        if len(thelist) > 1:
            self.l.error("Got multiple (%d) results for present filename %s "
                         "when getting md5sum from destination server",
                         (len(thelist), filename))
            return False

        try:
            self.destination_md5 = thelist[0]['data_md5']
        except (ValueError, KeyError):
            self.l.error("Error parsing md5 from json response", exc_info=True)
            return False

        self.destination_ingest_pending = \
            thelist[0].get("pending_ingest", False)

        self.l.debug("Got destination file info for filename %s: md5 %s, "
                     "pending_ingest: %s", filename, self.destination_md5,
                     self.destination_ingest_pending)
        return True
