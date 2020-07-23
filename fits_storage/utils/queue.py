import datetime
from sqlalchemy.orm import make_transient
from sqlalchemy.exc import OperationalError, IntegrityError
from ..orm.queue_error import QueueError, INGESTQUEUE, PREVIEWQUEUE, CALCACHEQUEUE, EXPORTQUEUE
import traceback
import linecache
from time import sleep

import re

date_re = re.compile(r'(\d{8}[SE]\d+)')

def sortkey_for_filename(filename):
    """
    Return a key to be used in the database sorting. Used mostly for queues, where
    filenames may be sorted in the wrong order because of the prefixes.

    Instead, it extracts the YYYYMMDDSNNNN from regular files and calibs, and
    returns that as the sorting key, prepended with a z to make sure that other 
    artifacts won't skew the order.

    If the filename doesn't include this information, we force it to the end of the
    queue by prepending an 'a' to it (the queue is popped in descending search order)
    """
    try:
        return 'z' + date_re.search(filename).groups()[0]
    except AttributeError:
        return 'a' + filename

def pop_queue(queue_class, session, logger, fast_rebuild=False):
    """
    Returns the next thing in the queue, and sets the inprogress flag on that entry.

    The ORM instance returned is detached from the database - it's a transient
    object not associated with the session. Basicaly treat it as a convenience
    dictionary, but don't try to modify the database with it.

    The select and update inprogress are done with a transaction lock
    to avoid race conditions or duplications when there is more than
    one process processing the ingest queue.

    Next to ingest is defined by a sort on the sortkey, which is
    the filename with the first character dropped off - so we effectively
    sort by date and frame number for raw data files.

    Also, when we go inprogress on an entry in the queue, we
    delete all other entries for the same filename.
    """

    tname = queue_class.__tablename__

    # NOTE: On acquiring data from the queue
    #
    # The following loop tries to get an exclusive lock on the data by using
    # SELECT ... FOR UPDATE NOWAIT. The reason for the 'NOWAIT' is to be able to distinguish the
    # two cases where the SELECT returns 'None': no matching data vs. data that matches but is
    # modified before we can acquire the lock.
    #
    # This operation can result in either:
    #
    #  1) Getting some object (there was something pending)
    #  2) Getting None (no pending objects)
    #  3) The operation raises OperationalError. This is because we ask to perform the SELECT with
    #     a NOWAIT clause, which returns immediately with an error in case that the lock cannot be
    #     acquired.
    #
    # Only 1) and 2) are desirable. If 3) happens, case we sleep for 1/20sec and try again.
    #
    # PostgreSQL 9.5 introduces SELECT ... FOR UPDATE SKIP LOCKED which removes the uncertainty of
    # the 'None' case. When (well, if) migrating to 9.5, refactor the loop to use that functionality.

    while True:
        try:
            # Running the code in a block with begin_nested() creates a SAVEPOINT and rolls back to
            # it in case of an exception.
            with session.begin_nested():
                qelement = queue_class.find_not_in_progress(session).with_for_update(of=queue_class, nowait=True).first()
                try:
                    # OK, we got a viable item, set it to inprogress and return it.
                    logger.debug("Popped id %d from %s", qelement.id, tname)
                    # Set this entry to in progres and flush to the DB.
                    qelement.inprogress = True
                    session.flush()
            
                    ##### There's no need to rebuild any longer
                    # if not fast_rebuild:
                    #     queue_class.rebuild(session, qelement)
                    #     session.flush()
            
                    # Make the qelement into a transient instance before we return it
                    # This detaches it from the session, basically it becomes a convenience container for the
                    # values (filename, path, etc). The problem is that if it's still attached to the session
                    # but expired (because we did a commit) then the next reference to it will initiate a transaction
                    # and a SELECT to refresh the values, and that transaction will then hold a FOR ACCESS SHARE lock
                    # on the exportqueue table until we complete the export and do a commit - which will prevent
                    # the ACCESS EXCLUSIVE lock in pop_exportqueue from being granted until the transfer completes.
                    make_transient(qelement)
                except AttributeError: # Got a None
                    logger.debug("No item to pop on %s", tname)
                break
        except OperationalError:
            sleep(0.05)


    # And we're done, commit the transaction and release the update lock
    session.commit()
    return qelement

def queue_length(queue_class, session):
    """
    return the length of the ingest queue
    """
    with session.begin_nested():
        return session.query(queue_class)\
                    .filter(queue_class.inprogress == False)\
                    .count()


def format_tb(tb):
    """
    Format a traceback as a human readable (ish) string

    Parameters
    ----------
    tb : exception traceback

    Returns
    -------
    str : text encoded dump of the stack trace
    """
    ret = []
    while tb is not None:
        f = tb.tb_frame
        lineno = tb.tb_lineno
        co = f.f_code
        filename = co.co_filename
        name = co.co_name
        ret.append('  File "%s", line %d, in %s' % (filename, lineno, name))
        linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        if line: ret.append('    ' + line.strip())
        tb = tb.tb_next

    return ret


def add_error(queue_class, obj, exc_type, exc_value, tb, session):
    """
    Add an error to the database.

    This helper adds an error in handling a queue item to the database for future
    triage.

    Parameters
    ----------
    queue_class : class of type :class:`~Queue`
        The Queue class to be logged
    obj : :class:`~Queue`
        The instance from the queue that failed
    exc_type : Any
        The type of the exception
    exc_value : Any
        The exception information
    tb : Any
        The traceback
    session : :class:`sqlalchemy.orm.session.Session`
        SQL Alchemy session to save the queue error to
    """
    session.rollback()
    queue = queue_class.error_name

    text = []

    if tb:
        text.append('Traceback (most recent call last):')
        text.extend(format_tb(tb))
    text.extend([x.rstrip() for x in  traceback.format_exception_only(exc_type, exc_value)])

    error = QueueError(obj.filename, obj.path, queue, '\n'.join(text))
    session.add(error)
    session.flush()

    try:
        attached_obj = session.merge(obj)
        attached_obj.failed = True
        attached_obj.inprogress = False
        session.commit()
    except IntegrityError:
        session.rollback()
        attached_obj = session.merge(obj)
        session.delete(attached_obj)
        session.commit()

# def set_error(queue_class, oid, exc_type, exc_value, tb, session):
#     session.rollback()
#     dbob = session.query(queue_class).get(oid)
#
#     text = []
#
#     if tb:
#         text.append('Traceback (most recent call last):')
#         text.extend(format_tb(tb))
#     text.extend([x.rstrip() for x in  traceback.format_exception_only(exc_type, exc_value)])
#
#     dbob.error = '\n'.join(text)
#     session.commit()


def delete_with_id(queue_class, oid, session):
    """
    Delete the queue entry by id

    Parameters
    ----------
    queue_class : class of type :class:`~Queue`
        The queue type to remove the record from
    oid : int
        The ID of the entry to remove
    session : :class:`sqlalchemy.orm.session.Session`
        SQL Alchemy session to delete the record from
    """
    dbob = session.query(queue_class).get(oid)
    session.delete(dbob)
    session.commit()
