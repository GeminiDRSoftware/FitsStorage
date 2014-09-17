"""
This module provides various utility functions for create_tables.py
in the Fits Storage System.
"""
import sqlalchemy

from fits_storage_config import using_apache, using_sqlite
from . import pg_db
from orm.file import File
from orm.diskfile import DiskFile
from orm.diskfilereport import DiskFileReport
from orm.fulltextheader import FullTextHeader
from orm.header import Header
from orm.footprint import Footprint
from orm.gmos import Gmos
from orm.niri import Niri
from orm.gnirs import Gnirs
from orm.nifs import Nifs
from orm.f2 import F2
from orm.michelle import Michelle
from orm.ingestqueue import IngestQueue
from orm.tapestuff import Tape, TapeWrite, TapeFile, TapeRead
from orm.notification import Notification
from orm.photstandard import PhotStandard
from orm.qastuff import QAreport, QAmetricIQ, QAmetricZP, QAmetricSB, QAmetricPE
from orm.authentication import Authentication
from orm.exportqueue import ExportQueue
from orm.user import User
from orm.userprogram import UserProgram
from orm.usagelog import UsageLog
from orm.querylog import QueryLog
from orm.downloadlog import DownloadLog
from orm.filedownloadlog import FileDownloadLog

def create_tables(session):
    """
    Creates the database tables and grants the apache user
    SELECT on the appropriate ones
    """
    # Create the tables
    File.metadata.create_all(bind=pg_db)
    DiskFile.metadata.create_all(bind=pg_db)
    DiskFileReport.metadata.create_all(bind=pg_db)
    FullTextHeader.metadata.create_all(bind=pg_db)
    Header.metadata.create_all(bind=pg_db)
    Footprint.metadata.create_all(bind=pg_db)
    Gmos.metadata.create_all(bind=pg_db)
    Niri.metadata.create_all(bind=pg_db)
    Nifs.metadata.create_all(bind=pg_db)
    Gnirs.metadata.create_all(bind=pg_db)
    F2.metadata.create_all(bind=pg_db)
    IngestQueue.metadata.create_all(bind=pg_db)
    Michelle.metadata.create_all(bind=pg_db)
    Tape.metadata.create_all(bind=pg_db)
    TapeWrite.metadata.create_all(bind=pg_db)
    TapeFile.metadata.create_all(bind=pg_db)
    TapeRead.metadata.create_all(bind=pg_db)
    Notification.metadata.create_all(bind=pg_db)
    PhotStandard.metadata.create_all(bind=pg_db)
    QAreport.metadata.create_all(bind=pg_db)
    QAmetricIQ.metadata.create_all(bind=pg_db)
    QAmetricZP.metadata.create_all(bind=pg_db)
    QAmetricSB.metadata.create_all(bind=pg_db)
    QAmetricPE.metadata.create_all(bind=pg_db)
    Authentication.metadata.create_all(bind=pg_db)
    ExportQueue.metadata.create_all(bind=pg_db)
    User.metadata.create_all(bind=pg_db)
    UserProgram.metadata.create_all(bind=pg_db)
    UsageLog.metadata.create_all(bind=pg_db)
    QueryLog.metadata.create_all(bind=pg_db)
    DownloadLog.metadata.create_all(bind=pg_db)
    FileDownloadLog.metadata.create_all(bind=pg_db)


    # Add the geometry types separately. this is postgres specific and referencing these column in local mode isn't going to work
    # Ignore any errors, commonly from column already exists...
    if not using_sqlite:
        try:
            session.execute("ALTER TABLE footprint ADD COLUMN area polygon")
            session.commit()
            session.execute("ALTER TABLE photstandard ADD COLUMN coords point")
            session.commit()
        except sqlalchemy.exc.ProgrammingError:
            pass

    if using_apache and not using_sqlite:
        # Now grant the apache user select on them for the www queries
        session.execute("GRANT SELECT ON file, diskfile, diskfilereport, header, fulltextheader, gmos, niri, michelle, gnirs, nifs, f2, tape, tape_id_seq, tapewrite, taperead, tapefile, notification, photstandard, photstandardobs, footprint, qareport, qametriciq, qametriczp, qametricsb, qametricpe, authentication, ingestqueue, exportqueue, archiveuser, userprogram, usagelog, querylog, downloadlog, filedownloadlog TO apache")
        session.commit()
        session.execute("GRANT INSERT,UPDATE ON tape, tape_id_seq, notification, notification_id_seq, qareport, qareport_id_seq, qametriciq, qametriciq_id_seq, qametriczp, qametriczp_id_seq, qametricsb, qametricsb_id_seq, qametricpe, qametricpe_id_seq, authentication, authentication_id_seq, archiveuser, archiveuser_id_seq, userprogram, userprogram_id_seq, usagelog, usagelog_id_seq, querylog, querylog_id_seq, downloadlog, downloadlog_id_seq, filedownloadlog, filedownloadlog_id_seq TO apache")
        session.commit()
        session.execute("GRANT DELETE ON notification TO apache")
        session.commit()

def drop_tables(session):
    """
    Drops all the database tables. Very unsubtle. Use with caution
    """
    File.metadata.drop_all(bind=pg_db)
    session.commit()
