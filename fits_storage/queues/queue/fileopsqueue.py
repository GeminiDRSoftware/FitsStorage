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
