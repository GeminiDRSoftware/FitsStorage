"""
This module provides various utility functions for create_tables.py
in the Fits Storage System.
"""
from sqlalchemy.orm import Session

import fits_storage.db as db
from fits_storage.config import get_config
fsc = get_config()

# Importing orm classes here (or even within imports that get called from
# here) will cause those tables to be created even though there is no
# reference to the orm class and it looks like the import is unused. When the
# orm class imports, it registers itself with the sqlalchemy engine, and that
# is enough to cause any metadata.create_all() call to create that table.

# Core ORM classes
from fits_storage.core.header import Header
from fits_storage.core.diskfilereport import DiskFileReport
from fits_storage.core.footprint import Footprint
from fits_storage.core.fulltextheader import FullTextHeader
from fits_storage.core.photstandard import PhotStandard

# Calibration instrument classes
from fits_storage.cal import instruments

# Server tables
if fsc.is_server:
    from fits_storage import server
    from fits_storage.queues.orm.ingestqueueentry import IngestQueueEntry
    from fits_storage.queues.orm.exportqueueentry import ExportQueueEntry
    from fits_storage.queues.orm.previewqueueentry import PreviewQueueEntry
    from fits_storage.queues.orm.calcachequeueentry import CalCacheQueueEntry

def create_tables(session: Session):
    """
    Creates the database tables appropriate for the configuration.
    If we're in a server configuration, also handles granting persmissions
    for the databse user that the web queries run under.

    Parameters
    ----------
    session : :class:`Session`
        Session to create tables in
    """

    # Create the tables. You only need to call create_all on one orm object,
    # and all tables for all imported ORM classes will be created.
    Header.metadata.create_all(bind=db._saved_engine)

    # Add the geometry types separately, because SQLAlchemy only partially
    # supports these. TODO: Is this still true?
    # This is postgres specific and referencing these column in sqlite mode
    # isn't going to work.
    if not fsc.using_sqlite:
        db._saved_engine.execute("ALTER TABLE footprint ADD IF NOT EXISTS area polygon;")
        db._saved_engine.execute("ALTER TABLE photstandard ADD IF NOT EXISTS coords point;")

    # Grant access to server tables for the unprivelidged user that runs the
    # wsgi code for the web server
    # TODO: this needs sorting out.
    # if using_apache and not using_sqlite:
    # pg_db.execute("GRANT SELECT ON file, diskfile, diskfilereport, header, fulltextheader, gmos, niri, michelle, gnirs, gpi, nifs, f2, gsaoi, nici, tape, tape_id_seq, tapewrite, taperead, tapefile, notification, photstandard, photstandardobs, footprint, qareport, qametriciq, qametriczp, qametricsb, qametricpe, ingestqueue, exportqueue, archiveuser, userprogram, usagelog, querylog, downloadlog, filedownloadlog, fileuploadlog, calcache, preview, obslog, miscfile, ingestqueue, exportqueue, previewqueue, calcachequeue, queue_error, obslog_comment, program, publication, programpublication TO fitsdata;")
    # pg_db.execute("GRANT INSERT,UPDATE ON tape, notification, qareport, qametriciq, qametriczp, qametricsb, qametricpe, archiveuser, userprogram, usagelog, querylog, downloadlog, filedownloadlog, fileuploadlog, miscfile, ingestqueue, obslog_comment, program, publication, programpublication TO fitsdata;")
    # pg_db.execute("GRANT UPDATE ON tape_id_seq, notification_id_seq, qareport_id_seq, qametriciq_id_seq, qametriczp_id_seq, qametricsb_id_seq, qametricpe_id_seq, archiveuser_id_seq, userprogram_id_seq, usagelog_id_seq, querylog_id_seq, downloadlog_id_seq, filedownloadlog_id_seq, fileuploadlog_id_seq, ingestqueue_id_seq, obslog_comment_id_seq, program_id_seq, publication_id_seq, programpublication_id_seq TO fitsdata;")
    # pg_db.execute("GRANT DELETE ON notification, ingestqueue TO fitsdata;")
def drop_tables(session: Session):
    """
    Drops all the database tables. Very unsubtle. Use with caution

    Parameters
    ----------
    session : :class:`Session`
        Session to create tables in
    """
    Header.metadata.drop_all(bind=db._saved_engine)
