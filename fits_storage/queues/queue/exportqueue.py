"""
ExportQueue housekeeping class. Note that this is not the ORM class, which is
now called ...QueueEntry as it represents an entry on the queue as opposed to
the queue itself.
"""

from sqlalchemy.exc import IntegrityError

from .queue import Queue
from ..orm.exportqueueentry import ExportQueueEntry


class ExportQueue(Queue):

    def __init__(self, session, logger=None):
        super().__init__(session, ormclass=ExportQueueEntry, logger=logger)

    def add(self, filename, path, destination, after=None,
            md5_before_header_update=None, md5_after_header_update=None,
            header_update=None):
        """
        Add an entry to the export queue. This instantiates an ExportQueueEntry
        object using the parameters passed, and adds it to the database.

        See ExportQueueEntry.__init__() for details of the parameters

        Returns
        -------
        False on error
        True on success
        """

        eqe = ExportQueueEntry(filename, path, destination, after=after,
                               header_update=header_update,
                               md5_before_header_update=md5_before_header_update,
                               md5_after_header_update=md5_after_header_update)

        self.session.add(eqe)

        try:
            self.session.commit()
            return True
        except IntegrityError:
            self.logger.debug(f"Integrity error adding file {filename} "
                              f"to Export Queue. Most likely, file is already"
                              f"on queue. Silently rolling back.")
            self.session.rollback()
            return False
