"""
This module implements fileops actions. It is called by
service_fileops_queue.py to carry out operations on the fits files.

Note there can only be one action per request - ie the request json
document must be exactly one dictionary as follows:
{
'request': 'name of request',
'args': {'dict': 'of', 'arguments': 10}
}

We will generate a response document containing exactly one
dictionary as follows:
{
'ok': True,
'error': '',
'value': 'Have a nice day'
}

If there is an error, the error message will be in 'error' and 'value' may
contain debugging or further information. If there was no error, 'error' will
be an empty string, and 'value' will contain the result.

This module contains the infrastructure, the functions providing the actual
functionality are imported from server.fileops
"""

import json
from fits_storage.queues.queue.fileopsqueue import FileOpsResponse
from fits_storage.server.fileops import FileOpsError

from .fileops import echo
from .fileops import ingest_upload, update_headers


class FileOpser(object):
    """
    This class provides the functionality for file operations. We instantiate
    this class in service_fileops_queue.py once and feed it fileopsqueueentry
    instances one at a time by calling fileop()
    """

    def __init__(self, session, logger):
        """
        Instantiate the FileOpser class with a session and logger

        Parameters
        ----------
        session - database session
        logger - Fits Storage Logger
        """

        self.s = session
        self.l = logger

        # This dictionary provides the lookup for the function to call.
        # Each worker function must accept a single argument which is a
        # dict of actual arguments
        self.workers = {
            'echo': echo,
            'ingest_upload': ingest_upload,
            'update_headers': update_headers
        }

        # These are per-operation values stored centrally for convenience.
        # They are reset between ops with reset()
        self.fqe = None
        self.request_name = None
        self.request_args = None
        self.worker = None
        self.response = None
        self.reset()

    def reset(self):
        self.fqe = None
        self.request_name = None
        self.request_args = None
        self.worker = None
        self.response = FileOpsResponse()

    def doerror(self, message, exc_info=False):
        self.l.error(message, exc_info=exc_info)
        self.response.ok = False
        self.response.error = message
        self.fqe.response = self.response.json()
        self.s.commit()

    def fileop(self, fqe):
        """
        Take a FileopsQueueEntry, decode the request, call the function to do
        the request, put the return value in the response and commit the
        database.

        If this is a response_required=True entry, then don't delete it from
        the queue when we're done - the "caller" - ie the thing that put it
        on the queue is responsible for that in this case.

        If this is a response_required=False entry, delete the queue entry
        if we complete successfully.
        """
        self.fqe = fqe

        # Any time this is called, a response *must* be added to the row
        # and committed. The doerror function does do this.

        # The request should be a json document, try and decode it.
        try:
            req = json.loads(fqe.request)
        except json.JSONDecodeError:
            self.doerror(f"Error decoding JSON request document: {fqe.request}",
                         exc_info=True)
            return

        # Get the request name (ie what they want doing)
        try:
            self.request_name = req['request']
        except KeyError:
            self.doerror("Request dict must contain request key", exc_info=True)
            return

        # Get the request arguments
        try:
            self.request_args = req['args']
        except KeyError:
            self.doerror("Request dict must contain args key", exc_info=True)
            return
        if not isinstance(self.request_args, dict):
            self.doerror("Request args must be a dict")
            return

        # If the entry has a (processing) batch, add that to the args
        if self.fqe.batch:
            self.request_args['batch'] = self.fqe.batch

        # Find which function to call to do that.
        try:
            self.worker = self.workers[self.request_name]
        except KeyError:
            self.doerror(f"No worker function for request: {self.request_name}",
                         exc_info=True)
            return

        # Call the worker function!
        try:
            self.response.value = self.worker(self.request_args, self.s, self.l)
            self.response.ok = True
        except FileOpsError as foe:
            self.doerror(str(foe))
            self.response.ok = False
        except Exception as err:
            err_msg = f"{err.__class__}: {err}"
            self.doerror("Exception calling worker function for "
                         f"{self.request_name} - {self.request_args}",
                         exc_info=True)
            return

        # It worked!
        fqe.response = self.response.json()

        if fqe.response_required is False:
            self.s.delete(fqe)

        self.s.commit()
        return
