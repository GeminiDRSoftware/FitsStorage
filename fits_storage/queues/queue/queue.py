"""
This is a generic Fits Storage Queue class. It provides housekeeping
functions that are common to all the queues. Some or all queues may subclass
this in order to provide queue specific functionality. Note that these are
*not* the ORM classes - those are now called eg ExportQueueEntry
as instances of those represent entries in the queue, not the queue itself.
"""
import datetime
from sqlalchemy import desc
from sqlalchemy.orm import make_transient

class Queue(object):

    def __init__(self, session, ormclass=None, logger=None):
        """
        Parameters
        ----------
        session - SQLalchemy database session
        ormclass - the ORM class for the queue
        logger - a FitsStorageLogger instance for log messages

        """
        self.session = session
        self.ormclass = ormclass
        self.logger = logger

    def length(self, include_inprogress=False):
        with self.session.begin_nested():
            query = self.session.query(self.ormclass)

            if not include_inprogress:
                query = query.filter(self.ormclass.inprogress == False)

            return query.count()

    def add(self, item):
        """
        Add an item to the queue.
        """

    def pop(self, fast_rebuild=False):
        """
        Pop an entry from the queue. We use select-for-update to ensure that
        this works correctly with multiple clients attempting to pop the
        queue simultaneously.

        The ORM instance returned is detached from the database - it's a
        transient object not associated with the session. It has all the data
        items from the ORM, but don't try to modify the database with it.

        There are lots of subelties to this. There could be duplicate entries
        in the queue (that's fine), some of which may be marked as failed and
        some may be inprogress. If there are two entries for the same file,
        one in progress and one not, we do not want to pop the second one
        until the first one completed, as that would be a race condition.

        So we first create a list of the filenames that are inprogress (but
        not marked as failed), and we exclude that from the selection criteria.

        """
        # for brevity:
        session = self.session
        ormclass = self.ormclass

        # Note - we make the qentry into a transient instance before we
        # return it. This detaches it from the session - basically it becomes
        # a convenience container for the values (filename, etc). The problem
        # is that if it's still attached to the session but expired (because
        # we did a commit) then the next reference to it (to process the
        # qentry) will initiate a transaction and a SELECT to refresh the
        # values, and that transaction will then hold a FOR ACCESS SHARE lock
        # on the table until we complete the processing and do a commit -
        # which will block the ACCESS EXCLUSIVE (or FOR UPDATE?) lock from
        # being granted (to pop another item) until the transfer completes.
        # I'm not sure if this is really an issue now we're using SELECT FOR
        # UPDATE rather than grabbing a whole ACCESS EXCLUSIVE lock,
        # but having it as transient makes sense anyway.

        # Wrap the whole thing in a savepoint so it can roll back cleanly
        # if there's an exception
        with session.begin_nested():

            # First build the query for the inprogress filenames list to
            # exclude. Note, this does not execute this query, that's done
            # within the main query under the select-for-update lock.

            inprogress_filenames = session.query(ormclass).\
                filter(ormclass.failed == False).\
                filter(ormclass.inprogress == True)


            query = session.query(ormclass).\
                filter(ormclass.inprogress == False).\
                filter(ormclass.failed == False).\
                filter(ormclass.after < datetime.datetime.utcnow()).\
                filter(~ormclass.filename.in_(inprogress_filenames)).\
                with_for_update(skip_locked=True).\
                order_by(desc(ormclass.sortkey))

            qentry = query.first()

            if qentry:
                self.logger.debug(f"Popped id {qelement.id} from "
                                  f"{ormclass.__tablename__}")
                # Set to inprogress and flush to the DB.
                qentry.inprogress = True
                session.flush()

                # Make the qentry transient (see comment above)
                make_transient(qentry)
            else:
                # Got None from .first()
                self.logger.debug(f"No item to pop on {ormclass.__tablename__}")

        session.commit()
        return qentry