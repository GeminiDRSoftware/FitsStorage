"""
IngestQueue housekeeping class. Note that this is not the ORM class, which is
now called IngestQueueEntry as it represents an entry on the queue as opposed to
the queue itself.
"""

import json
from sqlalchemy.exc import IntegrityError

from .queue import Queue
from ..orm.ingestqueueentry import IngestQueueEntry


class IngestQueue(Queue):

    def __init__(self, session, logger=None):
        super().__init__(session, ormclass=IngestQueueEntry, logger=logger)

    def add(self, filename, path, force_md5=False, force=False, after=None,
            header_update=None, md5_before_header_update=None,
            md5_after_header_update=None):
        """
        Add an entry to the ingest queue. This instantiates an IngestQueueEntry
        object using the arguments passed, and adds it to the database.

        The header_update items are populated when this ingest-queue add
        results from an API header update. When we add this to our export
        queues, we can pass on the header_update items as a hint which may
        facilitate replicating the destination by calling the header update
        API on the destination rather than re-transmitting the entire file.

        Parameters
        ----------
        filename
        path
        force_md5
        force
        after
        header_update
        md5_before_header_update
        md5_after_header_update

        Returns
        -------
        False on error
        True on success
        """

        # If we're getting a header update dict, store it as JSON.
        # TODO: Should we have the caller do this?
        if header_update is not None and isinstance(header_update, dict):
            header_update = json.dumps(header_update)

        iqe = IngestQueueEntry(filename, path, force=force, force_md5=force_md5,
                              after=after, header_update=header_update,
                              md5_before_header_update=md5_before_header_update,
                              md5_after_header_update=md5_after_header_update)

        self.session.add(iqe)
        try:
            self.session.commit()
            return True
        except IntegrityError:
            self.logger.debug(f"Integrity error adding file {filename} "
                              f"to Ingest Queue. Most likely, file is already"
                              f"on queue. Silently rolling back.")
            self.session.rollback()
            return False
