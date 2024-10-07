"""
PreviewQueue housekeeping class. Note that this is not the ORM class, which is
now called ...QueueEntry as it represents an entry on the queue as opposed to
the queue itself.
"""

from .queue import Queue
from ..orm.previewqueueentry import PreviewQueueEntry

from sqlalchemy.exc import IntegrityError

class PreviewQueue(Queue):

    def __init__(self, session, logger=None):
        super().__init__(session, ormclass=PreviewQueueEntry, logger=logger)

    def add(self, diskfile):
        """
        Add and entry to the PreviewQueue. This instantiates a
        PreviewQueueEntry instance, adds it to the database and commits
        the session.

        Returns
        -------
        False on error
        True on success
        """
        pqe = PreviewQueueEntry(diskfile)

        self.session.add(pqe)

        try:
            self.session.commit()
            return True
        except IntegrityError:
            self.logger.debug(f"Integrity error adding diskfile to "
                              f"PreviewQueue. Most likely, file is already"
                              f"on queue. Silently rolling back.")
            self.session.rollback()
            return False
