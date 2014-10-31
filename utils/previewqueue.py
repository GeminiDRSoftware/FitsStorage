"""
This module provides various utility functions to
manage and service the preview queue
"""
import os
import datetime
from logger import logger
from sqlalchemy import desc
from sqlalchemy.orm.exc import ObjectDeletedError
from sqlalchemy.orm import make_transient

from orm.preview import Preview
from orm.previewqueue import PreviewQueue

from utils.preview import make_preview

from fits_storage_config import using_s3, storage_root, preview_path


def pop_previewqueue(session, fast_rebuild=False):
    """
    Returns the next thing to ingest off the queue, and sets the
    inprogress flag on that entry.

    The ORM instance returned is detached from the database - it's a transient
    object not associated with the session. Basicaly treat it as a convenience
    dictionary for the diskfile_id etc, but don't try to modify the database with it.

    The select and update inprogress are done with a transaction lock
    to avoid race conditions or duplications when there is more than
    one process processing the ingest queue.

    The queue is ordered by the sortkey, which is the filename

    Also, when we go inprogress on an entry in the queue, we
    delete all other entries for the same filename.
    """

    # Is there a way to avoid the ACCESS EXCLUSIVE lock, especially with 
    # fast_rebuild where we are not changing other columns. Seemed like
    # SELECT FOR UPDATE ought to be able to do this, but it doesn't quite
    # do what we want as other threads can still select that row?

    session.execute("LOCK TABLE previewqueue IN ACCESS EXCLUSIVE MODE;")

    query = session.query(PreviewQueue).filter(PreviewQueue.inprogress == False)
    query = query.order_by(desc(PreviewQueue.sortkey))

    pq = query.first()
    if pq is None:
        logger.debug("No item to pop on preview queue")
    else:
        # OK, we got a viable item, set it to inprogress and return it.
        logger.debug("Popped id %d from preview queue", pq.id)
        # Set this entry to in progres and flush to the DB.
        pq.inprogress = True
        session.flush()

        if not fast_rebuild:
            # Find other instances and delete them
            others = session.query(PreviewQueue)
            others = others.filter(PreviewQueue.inprogress == False)
            others = others.filter(PreviewQueue.diskfile_id == pq.diskfile_id)
            others.delete()

        # Make the pq into a transient instance before we return it
        # This detaches it from the session, basically it becomes a convenience container for the
        # values (diskfile_id, etc). The problem is that if it's still attached to the session
        # but expired (because we did a commit) then the next reference to it will initiate a transaction
        # and a SELECT to refresh the values, and that transaction will then hold a FOR ACCESS SHARE lock
        # on the exportqueue table until we complete the export and do a commit - which will prevent
        # the ACCESS EXCLUSIVE lock in pop_exportqueue from being granted until the transfer completes.
        make_transient(pq)

    # And we're done, commit the transaction and release the update lock
    session.commit()
    return pq

def previewqueue_length(session):
    """
    return the length of the preview queue
    """
    length = session.query(PreviewQueue).filter(PreviewQueue.inprogress == False).count()
    # Even though there's nothing to commit, close the transaction
    session.commit()
    return length


def make_preview(session, diskfile_id):
    """
    Make the preview file and store it, and add the entry to the preview table
    """

    # Get the Diskfile object
    query = session.query(Diskfile).filter(Diskfile.id == diskfile_id)
    diskfile = query.one()

    if using_s3:
        # Get the preview data into a buffer, then store it to s3
        pass
    else:
        # Make the preview filename
        preview_filename = diskfile.filename + "_preview.jpg"
        preview_fullpath = os.path.join(storage_root, preview_path, preview_filename)
        fp = open(preview_fullpath, 'w')
