import datetime
from sqlalchemy.orm import make_transient
import re

date_re = re.compile(r'(\d{8}S\d{4})')

def sortkey_for_filename(filename):
    """
    Return a key to be used in the database sorting. Used mostly for queues, where
    filenames may be sorted in the wrong order because of the prefixes.

    Instead, it extracts the YYYYMMDDSNNNN from regular files and calibs, and
    returns that as the sorting key, to make sure that other artifacts won't skew
    the order.

    If the filename doesn't include this information, we force it to the end of the
    queue by prepending a 'z' to it.
    """
    try:
        return date_re.search(filename).groups()[0]
    except AttributeError:
        return 'z' + filename

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

    # Is there a way to avoid the ACCESS EXCLUSIVE lock, especially with 
    # fast_rebuild where we are not changing other columns. Seemed like
    # SELECT FOR UPDATE ought to be able to do this, but it doesn't quite
    # do what we want as other threads can still select that row?

    tname = queue_class.__tablename__

    session.execute("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE;".format(tname))

    qelement = queue_class.find_not_in_progress(session).first()
    try:
        # OK, we got a viable item, set it to inprogress and return it.
        logger.debug("Popped id %d from %s", qelement.id, tname)
        # Set this entry to in progres and flush to the DB.
        qelement.inprogress = True
        session.flush()

        if not fast_rebuild:
            queue_class.rebuild(session, qelement)

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
