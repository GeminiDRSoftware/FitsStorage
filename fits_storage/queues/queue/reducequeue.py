"""ReduceQueue housekeeping class. Note that this is not the ORM class,
which is  called ReduceQueueEntry as it represents an entry on the queue
as opposed to the queue itself."""

from sqlalchemy.exc import IntegrityError

from .queue import Queue
from ..orm.reducequeentry import ReduceQueueEntry


class ReduceQueue(Queue):

    def __init__(self, session, logger=None):
        super().__init__(session, ormclass=ReduceQueueEntry, logger=logger)

    def add(self, filenames, intent=None, initiatedby=None, tag=None,
            recipe=None, capture_files=True, capture_monitoring=True):
        """
        Add an entry to the reduce queue. This instantiates a ReduceQueueEntry
        object using the arguments passed, and adds it to the database.

        Parameters
        ----------
        filenames

        Returns
        -------
        None on error
        ReduceQueueEntry added on success
        """

        rqe = ReduceQueueEntry(filenames=filenames)
        rqe.initiatedby = initiatedby
        rqe.intent = intent
        rqe.tag = tag
        rqe.recipe = recipe

        self.session.add(rqe)
        try:
            self.session.commit()
            return rqe
        except IntegrityError:
            self.logger.debug(f"Integrity error adding files {filenames} "
                              "to Reduce Queue. Most likely, files are already"
                              "on queue. Silently rolling back.")
            self.session.rollback()
            return None
