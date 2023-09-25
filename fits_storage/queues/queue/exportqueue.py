"""
ExportQueue housekeeping class. Note that this is not the ORM class, which is
now called ...QueueEntry as it represents an entry on the queue as opposed to
the queue itself.
"""
import datetime

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

    def retry_failures(self, interval=60):
        """
        Mark any failed queue entries for re-try. We call this when the queue
        is empty in order to re-try anything that failed.

        This method finds any not-in-progress and failed export queue entries
        and marks them as not failed with an 'after' value 'interval' seconds
        in the future.
        """

        fails = self.session.query(ExportQueueEntry)\
            .filter(ExportQueueEntry.inprogress == False)\
            .filter(ExportQueueEntry.fail_dt != ExportQueueEntry.fail_dt_false)\
            .all()

        future = datetime.datetime.now() + datetime.timedelta(seconds=interval)

        for fail in fails:
            fail.fail_dt = fail.fail_dt_false
            fail.after = future

        self.session.commit()
        