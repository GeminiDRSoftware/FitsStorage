"""FileopsQueue housekeeping class. Note that this is not the ORM class."""
import datetime
import json
import time

from sqlalchemy.exc import IntegrityError

from .queue import Queue
from ..orm.fileopsqueueentry import FileopsQueueEntry


class FileopsQueue(Queue):

    def __init__(self, session, logger=None):
        super().__init__(session, ormclass=FileopsQueueEntry, logger=logger)

    def add(self, fo_reqest, filename=None, after=None,
            response_required=False):
        """
        Add an entry to the fileops queue. This instantiates a FileopsQueueEntry
        object using the arguments passed, and adds it to the database.

        Parameters
        ----------
        fo_reqest - FileopsRequest object
        filename (can be None)
        after
        response_required (Boolean)
        Returns
        -------
        fileops queue entry added. None on failure.
        """

        fqe = FileopsQueueEntry(fo_reqest.json(), filename=filename,
                                after=after,
                                response_required=response_required)

        self.session.add(fqe)
        try:
            self.session.commit()
            return fqe
        except IntegrityError:
            self.logger.debug("Integrity error adding request "
                              f"{fo_reqest.json()} to Fileops Queue. "
                              "Rolling back.")
            self.session.rollback()
            return None

    def poll_for_response(self, id, timeout=10):
        """
        Poll filequeue entry id until response is not null. Timeout value
        is in seconds.
        If we find a response, delete the queueentry and return a response
        instance for it.
        This is called by "clients" which add queue entries with
        response_required = True
        """
        now = datetime.datetime.utcnow()
        then = now + datetime.timedelta(seconds=timeout)

        query = self.session.query(FileopsQueueEntry).\
            filter(FileopsQueueEntry.id == id).\
            filter(FileopsQueueEntry.inprogress == True).\
            filter(FileopsQueueEntry.response != None)

        fqe = None
        while (datetime.datetime.utcnow() < then) and fqe is None:
            self.logger.debug("Polling fqe id %d", id)
            fqe = query.first()
            time.sleep(1)

        if fqe:
            self.session.refresh(fqe)
            self.logger.debug("Found FQE with response: %s", fqe.response)
            fo_resp = FileOpsResponse(json=fqe.response)
            self.session.delete(fqe)
            self.session.commit()
            return fo_resp
        else:
            self.logger.debug("Did not find FQE with response - timed out")
            return None


class FileOpsResponse(object):
    """Fileops response object. Stores values and (de)serializes as required"""
    ok = False
    error = None
    value = None

    def __init__(self, ok=False, error='', value='', json=None):
        self.ok = ok
        self.error = error
        self.value = value

        if json is not None:
            self.loads(json)

    def dict(self):
        return {'ok': self.ok,
                'error': self.error,
                'value': self.value}

    def json(self):
        return json.dumps(self.dict())

    def loads(self, jsondoc):
        """Load a json document string, parse the values"""
        resp = json.loads(jsondoc)
        self.ok = resp['ok']
        self.error = resp['error']
        self.value = resp['value']


class FileOpsRequest(object):
    """Fileops request object. Stores values and (de)serializes as required"""
    request = None
    args = None

    def __init__(self, request='', args={}):
        self.request = request
        self.args = args

    def dict(self):
        return {'request': self.request,
                'args': self.args}

    def json(self):
        return json.dumps(self.dict())

    def loads(self, jsondoc):
        """Load a json document string, parse the values"""
        resp = json.loads(jsondoc)
        self.request = resp['request']
        self.args = resp['args']
