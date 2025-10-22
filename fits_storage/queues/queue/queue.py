"""
This is a generic Fits Storage Queue class. It provides housekeeping
functions that are common to all the queues. Some or all queues may subclass
this in order to provide queue specific functionality. Note that these are
*not* the ORM classes - those are now called e.g. ExportQueueEntry
as instances of those represent entries in the queue, not the queue itself.
"""
import datetime
from sqlalchemy import desc


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

    def pop(self):
        """
        Pop an entry from the queue. We use select-for-update to ensure that
        this works correctly with multiple clients attempting to pop the
        queue simultaneously.

        There are lots of subtleties to this. There could be duplicate entries
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

        # Note - in the past we would make the qentry a transient instance
        # before returning it because of issues with ACCESS EXCLUSIVE locking.
        # Now that SQLAlchemy supports (and we're using) SELECT FOR UPDATE,
        # that's no longer a factor, and we DO NOT make the qentries into
        # transient objects before returning them.

        # There's a quirk regarding the way failed is handled. See the note
        # in ormqueuemixin.py .Basically fail_dt == fail_dt_false
        # means failed == False

        # Wrap the whole thing in a savepoint, so it can roll back cleanly
        # if there's an exception
        with session.begin_nested():

            # First build the query for the inprogress filenames list to
            # exclude. Note, this does not execute this query, that's done
            # within the main query under the select-for-update lock.
            # Exclude null values from this list - entries will null filenames
            # do not care about filename collisions.

            inprogress_filenames = session.query(ormclass.filename).\
                filter(ormclass.fail_dt == ormclass.fail_dt_false).\
                filter(ormclass.inprogress == True).\
                filter(ormclass.filename != None)

            query = session.query(ormclass).\
                filter(ormclass.inprogress == False).\
                filter(ormclass.fail_dt == ormclass.fail_dt_false). \
                filter(~ormclass.filename.in_(inprogress_filenames))

            if hasattr(ormclass, 'after'):
                query = query.filter(ormclass.after <
                                     datetime.datetime.utcnow())

            query = query.with_for_update(skip_locked=True).\
                order_by(desc(ormclass.sortkey))

            qentry = query.first()

            if qentry is not None:
                self.logger.debug(f"Popped id {qentry.id} - {qentry.filename} "
                                  f"from {ormclass.__tablename__}")
                # Set to inprogress and flush to the DB.
                qentry.inprogress = True
                session.flush()
            else:
                # Got None from .first()
                self.logger.debug(f"No item to pop on {ormclass.__tablename__}")

        session.commit()
        return qentry
