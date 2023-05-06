"""FileopsQueue housekeeping class. Note that this is not the ORM class."""

import json
from sqlalchemy.exc import IntegrityError

from .queue import Queue
from ..orm.fileopsqueueentry import FileopsQueueEntry

class FileopsQueue(Queue):

    def __init__(self, session, logger=None):
        super().__init__(session, ormclass=FileopsQueueEntry, logger=logger)

    def add(self, request, after=None):
        """
        Add an entry to the fileops queue. This instantiates a FileopsQueueEntry
        object using the arguments passed, and adds it to the database.

        Parameters
        ----------
        request
        after

        Returns
        -------
        False on error
        True on success
        """

        fqe = FileopsQueueEntry(request, after=after)

        self.session.add(fqe)
        try:
            self.session.commit()
            return True
        except IntegrityError:
            self.logger.debug(f"Integrity error adding request {request} "
                              f"to Fileops Queue. Silently rolling back.")
            self.session.rollback()
            return False

class FileOpsResponse(object):
    """Fileops response object. Stores values and (de)serializes as required"""
    ok = False
    error = None
    value = None

    def __init__(self, ok=False, error='', value=''):
        self.ok = ok
        self.error = error
        self.value = value

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