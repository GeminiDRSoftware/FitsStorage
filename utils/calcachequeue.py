"""
This module provides various utility functions to
manage and service the calcache queue
"""
import os
import datetime
from logger import logger
from sqlalchemy import desc
from sqlalchemy.orm.exc import ObjectDeletedError
from sqlalchemy.orm import make_transient

from orm.header import Header
from orm.calcache import CalCache
from orm.calcachequeue import CalCacheQueue

from cal import get_cal_object
from cal.associate_calibrations import associate_cals

def pop_calcachequeue(session, fast_rebuild=False):
    """
    Returns the next thing to ingest off the queue, and sets the
    inprogress flag on that entry.

    The ORM instance returned is detached from the database - it's a transient
    object not associated with the session. Basicaly treat it as a convenience
    dictionary for the obs_hid etc, but don't try to modify the database with it.

    The select and update inprogress are done with a transaction lock
    to avoid race conditions or duplications when there is more than
    one process processing the ingest queue.

    The queue is ordered by the sortkey, which is the observation ut datetime

    Also, when we go inprogress on an entry in the queue, we
    delete all other entries for the same filename.
    """

    # Is there a way to avoid the ACCESS EXCLUSIVE lock, especially with 
    # fast_rebuild where we are not changing other columns. Seemed like
    # SELECT FOR UPDATE ought to be able to do this, but it doesn't quite
    # do what we want as other threads can still select that row?

    session.execute("LOCK TABLE calcachequeue IN ACCESS EXCLUSIVE MODE;")

    query = session.query(CalCacheQueue).filter(CalCacheQueue.inprogress == False)
    query = query.order_by(desc(CalCacheQueue.sortkey))

    ccq = query.first()
    if ccq is None:
        logger.debug("No item to pop on cal cache queue")
    else:
        # OK, we got a viable item, set it to inprogress and return it.
        logger.debug("Popped id %d from calcachequeue", ccq.id)
        # Set this entry to in progres and flush to the DB.
        ccq.inprogress = True
        session.flush()

        if not fast_rebuild:
            # Find other instances and delete them
            others = session.query(CalCacheQueue)
            others = others.filter(CalCacheQueue.inprogress == False)
            others = others.filter(CalCacheQueue.obs_hid == ccq.obs_hid)
            others.delete()

        # Make the ccq into a transient instance before we return it
        # This detaches it from the session, basically it becomes a convenience container for the
        # values (obs_hid, etc). The problem is that if it's still attached to the session
        # but expired (because we did a commit) then the next reference to it will initiate a transaction
        # and a SELECT to refresh the values, and that transaction will then hold a FOR ACCESS SHARE lock
        # on the exportqueue table until we complete the export and do a commit - which will prevent
        # the ACCESS EXCLUSIVE lock in pop_exportqueue from being granted until the transfer completes.
        make_transient(ccq)

    # And we're done, commit the transaction and release the update lock
    session.commit()
    return ccq

def calcachequeue_length(session):
    """
    return the length of the calcache queue
    """
    length = session.query(CalCacheQueue).filter(CalCacheQueue.inprogress == False).count()
    # Even though there's nothing to commit, close the transaction
    session.commit()
    return length


def cache_associations(session, obs_hid):
    """
    Do the calibration association and insert the associations into the calcache table.
    Remove any old associations that this replaces
    """

    # Get the Header object
    query = session.query(Header).filter(Header.id == obs_hid)
    header = query.one()

    if None in [header.instrument, header.ut_datetime]:
        return

    # Get a cal object for it
    cal = get_cal_object(session, None, header)

    # Loop through the applicable calibraiton types
    for caltype in cal.applicable:
        # Blow away old associations of this caltype
        query = session.query(CalCache).filter(CalCache.obs_hid == header.id)
        query = query.filter(CalCache.caltype == caltype)
        query.delete()

        # Get the associations for this caltype
        cal_headers = associate_cals(session, [header], caltype=caltype)
        rank = 0
        for cal_header in cal_headers:
            cc = CalCache(obs_hid, cal_header.id, caltype, rank)
            session.add(cc)
            rank += 1
        session.commit()
