"""ReduceQueue housekeeping class. Note that this is not the ORM class,
which is  called ReduceQueueEntry as it represents an entry on the queue
as opposed to the queue itself."""

import socket
import time
from sqlalchemy import select, text
from sqlalchemy.sql import func, desc
from sqlalchemy.exc import IntegrityError

from .queue import Queue
from ..orm.reducequeentry import ReduceQueueEntry

from fits_storage.config import get_config
fsc = get_config()

class ReduceQueue(Queue):

    def __init__(self, session, logger=None):
        super().__init__(session, ormclass=ReduceQueueEntry, logger=logger)
        self.server_gbs = fsc.get('reducer_memory_gb')
        try:
            # Set host to hostname
            self.host = socket.gethostname()
            # Try to look up IP address, in case multiple cloud instances
            # end up with the same hostname
            self.host = socket.gethostbyname(self.host)
        except socket.gaierror:
            # IP address lookup failed, just leave it at hostname
            pass

    def add(self, filenames, intent=None, initiatedby=None, tag=None,
            recipe=None, capture_files=True, capture_monitoring=True,
            debundle=None, mem_gb=0):
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
        rqe.capture_files = capture_files
        rqe.capture_monitoring = capture_monitoring
        rqe.debundle = debundle
        rqe.mem_gb = mem_gb

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

    def pop(self, logger=None):
        """
        Pop an entry from the queue. See the superclass version for more info

        Reduce queue does not have to worry about duplicate filenames
        inprogress, but does have to worry about memory footprint.

        There's a subtle but important difference between the reducequeue and
        the other queues - with the other queues we only need to lock the rows
        we are going to update, with reducequeue we need to lock everything,
        because we need to prevent simultaneous pop queries collectively
        over-shooting the memory constraint. Without a global lock, simultaneous
        'pop's can each pop and entry that by itself is within the memory
        constraint, but collectively exceed it because each is unaware that the
        otherone is about to set a row to inprogress that wasn't in progres when
        it checked memory use.

        There's a subtle gotcha with this in that with sqlalchemy, almost any
        access to the data values of an ORM instance will start a transaction -
        ie "BEGIN; SELECT blah.* from blah where id=..." and until that
        transaction ends (ie COMMIT;s) we cannot get an ACCESS EXCLUSIVE lock
        on the table. So in all the reducequeue and reducer code, we need to
        be diligent about doing a session.commit() after we are done accessing
        elements of the reducequeue instance, even if we didn't modify them.
        """

        # for brevity:
        session = self.session
        ormclass = self.ormclass

        with session.begin_nested():

            # Total GBs of entries not failed, in progress, and on this host.
            # coalesce is used to return 0 rather than NULL if no results.
            subq = (
                select(func.coalesce(func.sum(ReduceQueueEntry.mem_gb), 0.0))
                .where(ReduceQueueEntry.fail_dt == ReduceQueueEntry.fail_dt_false)
                .where(ReduceQueueEntry.inprogress == True)
                .where(ReduceQueueEntry.host == self.host)
                .scalar_subquery()
            )

            stmt = (
                select(ReduceQueueEntry)
                .where(ReduceQueueEntry.inprogress == False)
                .where(ReduceQueueEntry.fail_dt == ReduceQueueEntry.fail_dt_false)
                .where(ReduceQueueEntry.mem_gb < (self.server_gbs - subq))
                .order_by(desc(ReduceQueueEntry.sortkey))
                .limit(1)
            )

            requested_ns = time.monotonic_ns()
            session.execute(text("LOCK TABLE reducequeue IN ACCESS EXCLUSIVE MODE"))
            got_ns = time.monotonic_ns()
            qentry = session.scalars(stmt).first()

            if qentry is not None:
                self.logger.debug(f"Popped id {qentry.id} - {qentry.filename} "
                                  f"from {ormclass.__tablename__}")
                # Set to inprogress, set the host and flush to the DB.
                qentry.inprogress = True
                qentry.host = self.host
                session.flush()

            else:
                # Got None from .first()
                self.logger.debug(f"No item to pop on {ormclass.__tablename__}")

        # This commit releases the ACCESS EXCLUSIVE lock.
        session.commit()
        released_ns = time.monotonic_ns()
        if logger:
            waiting_ms = (got_ns - requested_ns) / 1E6
            held_ms = (released_ns - got_ns) / 1E6
            logger.debug(f"PopRQ: waiting for lock: {waiting_ms} ms, held lock: {held_ms} ms")
        return qentry
