"""
This module provides various utility functions for create_tables.py
in the Fits Storage System.
"""
from sqlalchemy.orm import Session

import gemini_obs_db.db as db
# from gemini_obs_db.db import pg_db
from gemini_obs_db.orm.file import File
from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.header import Header
from gemini_obs_db.orm.gmos import Gmos
from gemini_obs_db.orm.niri import Niri
from gemini_obs_db.orm.gnirs import Gnirs
from gemini_obs_db.orm.nifs import Nifs
from gemini_obs_db.orm.f2 import F2
from gemini_obs_db.orm.ghost import Ghost
from gemini_obs_db.orm.gpi import Gpi
from gemini_obs_db.orm.gsaoi import Gsaoi
from gemini_obs_db.orm.nici import Nici
from gemini_obs_db.orm.michelle import Michelle
from gemini_obs_db.orm.calcache import CalCache


def create_tables(session: Session):
    """
    Creates the database tables and grants the apache user
    SELECT on the appropriate ones

    Parameters
    ----------
    session : :class:`Session`
        Session to create tables in
    """
    # Create the tables
    File.metadata.create_all(bind=db.pg_db)
    DiskFile.metadata.create_all(bind=db.pg_db)
    Header.metadata.create_all(bind=db.pg_db)
    Gmos.metadata.create_all(bind=db.pg_db)
    Niri.metadata.create_all(bind=db.pg_db)
    Nifs.metadata.create_all(bind=db.pg_db)
    Gnirs.metadata.create_all(bind=db.pg_db)
    F2.metadata.create_all(bind=db.pg_db)
    Ghost.metadata.create_all(bind=db.pg_db)
    Gpi.metadata.create_all(bind=db.pg_db)
    Gsaoi.metadata.create_all(bind=db.pg_db)
    Michelle.metadata.create_all(bind=db.pg_db)
    Nici.metadata.create_all(bind=db.pg_db)
    CalCache.metadata.create_all(bind=db.pg_db)


def drop_tables(session: Session):
    """
    Drops all the database tables. Very unsubtle. Use with caution

    Parameters
    ----------
    session : :class:`Session`
        Session to create tables in
    """
    File.metadata.drop_all(bind=db.pg_db)
