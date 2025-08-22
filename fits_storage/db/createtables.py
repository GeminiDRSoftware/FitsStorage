"""
This module provides various utility functions for create_tables.py
in the Fits Storage System.
"""
from sqlalchemy import text
from sqlalchemy.orm import Session

import fits_storage.db as db
from fits_storage.config import get_config
fsc = get_config()

# This is a little helper class that makes dealing with database permissions
# a lot more tidy further down


class GrantHelper(object):
    _select = []
    _insert = []
    _update = []
    _delete = []

    def grant(self, perm, thing):
        if isinstance(thing, list):
            perm.extend(thing)
        else:
            perm.append(thing)

    def select(self, thing):
        self.grant(self._select, thing)

    def insert(self, thing):
        self.grant(self._insert, thing)

    def update(self, thing):
        self.grant(self._update, thing)

    def delete(self, thing):
        self.grant(self._delete, thing)

    @property
    def select_string(self):
        return ', '.join(self._select)

    @property
    def insert_string(self):
        return ', '.join(self._insert)

    @property
    def delete_string(self):
        return ', '.join(self._delete)

    @property
    def update_string(self):
        # We need to grant update on the _id_seq of any table we grant insert on
        id_seqs = [t + '_id_seq' for t in self._insert]
        return ', '.join(self._update + id_seqs)


# Importing orm classes here (or even within imports that get called from
# here) will cause those tables to be created even though there is no
# reference to the orm class and it looks like the import is unused. When the
# orm class imports, it registers itself with the sqlalchemy engine, and that
# is enough to cause any metadata.create_all() call to create that table.

# Core ORM classes
from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header
from fits_storage.core.orm.diskfilereport import DiskFileReport
from fits_storage.core.orm.footprint import Footprint
from fits_storage.core.orm.fulltextheader import FullTextHeader
from fits_storage.core.orm.photstandard import PhotStandard

# Calibration instrument classes
from fits_storage.cal import orm

# Server tables
if fsc.is_server:
    from fits_storage import server
    from fits_storage.queues.orm.ingestqueueentry import IngestQueueEntry
    from fits_storage.queues.orm.exportqueueentry import ExportQueueEntry
    from fits_storage.queues.orm.previewqueueentry import PreviewQueueEntry
    from fits_storage.queues.orm.calcachequeueentry import CalCacheQueueEntry
    from fits_storage.queues.orm.fileopsqueueentry import FileopsQueueEntry
    from fits_storage.queues.orm.reducequeentry import ReduceQueueEntry
    from fits_storage.server.orm.preview import Preview
    from fits_storage.server.orm.provenancehistory import Provenance, History
    from fits_storage.server.orm.miscfile import MiscFile
    from fits_storage.server.orm.user import User
    from fits_storage.server.orm.userprogram import UserProgram
    from fits_storage.server.orm.usagelog import UsageLog
    from fits_storage.server.orm.querylog import QueryLog
    from fits_storage.server.orm.downloadlog import DownloadLog
    from fits_storage.server.orm.filedownloadlog import FileDownloadLog
    from fits_storage.server.orm.fileuploadlog import FileUploadLog
    from fits_storage.server.orm.notification import Notification
    from fits_storage.server.orm.program import Program
    from fits_storage.server.orm.publication import Publication
    from fits_storage.server.orm.publication import ProgramPublication
    from fits_storage.server.orm.obslog import Obslog
    from fits_storage.server.orm.obslog_comment import ObslogComment
    from fits_storage.server.orm.reduction import Reduction
    from fits_storage.server.orm.monitoring import Monitoring
    from fits_storage.server.orm.processinglog import ProcessingLog
    from fits_storage.server.orm.processingtag import ProcessingTag


    from fits_storage.server.orm.qastuff import QAreport, \
        QAmetricIQ, QAmetricZP, QAmetricSB, QAmetricPE

    from fits_storage.server.orm.tapestuff import Tape, TapeWrite, TapeFile, TapeRead

# Archive specific tables
if fsc.is_archive:
    from fits_storage.cal.orm.calcache import CalCache
    from fits_storage.server.orm.ipprefix import IPPrefix
    from fits_storage.server.orm.usagelog_analysis import UsageLogAnalysis
    from fits_storage.server.orm.glacier import Glacier

def get_fitsweb_granthelper():
    # Define server database permissions here for clarity. Using helper class
    grant = GrantHelper()

    # Things that the fitsweb role needs select on for basic queries:
    grant.select(
        ['file', 'diskfile', 'diskfilereport', 'header', 'fulltextheader',
         'gmos', 'niri', 'michelle', 'gnirs', 'gpi', 'nifs', 'f2', 'gsaoi',
         'nici', 'ghost', 'photstandard', 'photstandardobs', 'footprint',
         'preview', 'obslog', 'miscfile', 'obslog_comment', 'program',
         'publication', 'programpublication', 'provenance', 'history',
         'reduction', 'processingtag', 'monitoring'])

    # For the notification system:
    grant.select('notification')
    grant.insert('notification')
    grant.update('notification')
    grant.delete('notification')

    # For log reporting:
    log_tables = ['usagelog', 'querylog', 'downloadlog', 'filedownloadlog',
                  'fileuploadlog']
    grant.select(log_tables)
    grant.insert(log_tables)
    grant.update(log_tables)

    # For the queue status page:
    grant.select(['ingestqueue', 'exportqueue', 'fileopsqueue', 'previewqueue',
                  'calcachequeue', 'reducequeue'])

    # For the qametric system:
    qametric_tables = ['qareport', 'qametriciq', 'qametriczp', 'qametricsb',
                       'qametricpe']
    grant.select(qametric_tables)
    grant.insert(qametric_tables)
    grant.update(qametric_tables)

    # For the tape system web interface:
    grant.select(['tape', 'tape_id_seq', 'tapewrite', 'taperead', 'tapefile'])
    grant.insert('tape')
    grant.update('tape')

    # For user management:
    user_tables = ['users', 'userprogram']
    grant.select(user_tables)
    grant.insert(user_tables)
    grant.update(user_tables)

    # For miscfiles
    grant.insert('miscfile')
    grant.update('miscfile')

    # For file upload
    grant.insert('fileopsqueue')

    # For stuff from the ODB and for publications
    odb_tables = ['obslog_comment', 'program', 'publication',
                  'programpublication']
    grant.insert(odb_tables)
    grant.update(odb_tables)

    # Archive specific tables
    if fsc.is_archive:
        grant.select('ipprefix')

    return grant

def get_dragons_granthelper():
    # Define server database permissions here for clarity. Using helper class
    grant = GrantHelper()

    # Reducequeue.
    grant.select('reducequeue')
    grant.update('reducequeue')
    grant.delete('reducequeue')
    grant.insert('reducequeue')
    grant.select('reducequeue_id_seq')
    grant.update('reducequeue_id_seq')

    # To find files
    grant.select('diskfile')

    # For Monitoring
    grant.select('header')
    grant.insert('monitoring')
    grant.update('monitoring')
    grant.select('monitoring')
    grant.select('monitoring_id_seq')
    grant.update('monitoring_id_seq')

    # For logging
    grant.select('processinglog')
    grant.insert('processinglog')
    grant.update('processinglog')
    grant.select('processinglog_id_seq')
    grant.update('processinglog_id_seq')


    return grant



def create_tables(session: Session):
    """
    Creates the database tables appropriate for the configuration.
    If we're in a server configuration, also handles granting permissions
    for the database user that the web queries run under.

    Parameters
    ----------
    session : :class:`Session`
        Session to create tables in
    """

    fsc = get_config()

    # Create the tables. You only need to call create_all on one orm object,
    # and all tables for all imported ORM classes will be created.
    File.metadata.create_all(bind=db._saved_engine)

    # Add the geometry types separately, because SQLAlchemy only partially
    # supports these. TODO: Is this still true?
    # This is postgres specific and referencing these column in sqlite mode
    # isn't going to work.
    if not fsc.using_sqlite:
            session.execute(text(("ALTER TABLE footprint ADD IF NOT EXISTS area polygon;")))
            session.execute(text("ALTER TABLE photstandard ADD IF NOT EXISTS coords point;"))

            # Grant access to server tables for the unprivileged user that runs the
            # wsgi code for the web server (ie 'fitsweb')
            if fsc.is_server:
                grant = get_fitsweb_granthelper()
                session.execute(text(f"GRANT SELECT ON {grant.select_string} TO fitsweb;"))
                session.execute(text(f"GRANT INSERT ON {grant.insert_string} TO fitsweb;"))
                session.execute(text(f"GRANT UPDATE ON {grant.update_string} TO fitsweb;"))
                session.execute(text(f"GRANT DELETE ON {grant.delete_string} TO fitsweb;"))

            if fsc.is_archive:
                session.execute(text("GRANT SELECT ON calcache TO fitsweb;"))

                grant = get_dragons_granthelper()
                session.execute(text(f"GRANT SELECT ON {grant.select_string} TO dragons;"))
                session.execute(text(f"GRANT INSERT ON {grant.insert_string} TO dragons;"))
                session.execute(text(f"GRANT UPDATE ON {grant.update_string} TO dragons;"))
                session.execute(text(f"GRANT DELETE ON {grant.delete_string} TO dragons;"))

            session.commit()


def drop_tables(session: Session):
    """
    Drops all the database tables. Very unsubtle. Use with caution

    Parameters
    ----------
    session : :class:`Session`
        Session to create tables in
    """
    File.metadata.drop_all(bind=db._saved_engine)
