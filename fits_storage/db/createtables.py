"""
This module provides various utility functions for create_tables.py
in the Fits Storage System.
"""
from sqlalchemy.orm import Session

import fits_storage.db as db
from fits_storage.orm.file import File
# from fits_storage.orm.diskfile import DiskFile
# from fits_storage.orm.header import Header
# from fits_storage.orm.gmos import Gmos
# from fits_storage.orm.niri import Niri
# from fits_storage.orm.gnirs import Gnirs
# from fits_storage.orm.nifs import Nifs
# from fits_storage.orm.f2 import F2
# from fits_storage.orm.ghost import Ghost
# from fits_storage.orm.gpi import Gpi
# from fits_storage.orm.gsaoi import Gsaoi
# from fits_storage.orm.nici import Nici
# from fits_storage.orm.michelle import Michelle
# from fits_storage.orm.calcache import CalCache


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
    File.metadata.create_all(bind=db._saved_engine)
    # DiskFile.metadata.create_all(bind=db._saved_engine)
    # Header.metadata.create_all(bind=db._saved_engine)
    # Gmos.metadata.create_all(bind=db._saved_engine)
    # Niri.metadata.create_all(bind=db._saved_engine)
    # Nifs.metadata.create_all(bind=db._saved_engine)
    # Gnirs.metadata.create_all(bind=db._saved_engine)
    # F2.metadata.create_all(bind=db._saved_engine)
    # Ghost.metadata.create_all(bind=db._saved_engine)
    # Gpi.metadata.create_all(bind=db._saved_engine)
    # Gsaoi.metadata.create_all(bind=db._saved_engine)
    # Michelle.metadata.create_all(bind=db._saved_engine)
    # Nici.metadata.create_all(bind=db._saved_engine)
    # CalCache.metadata.create_all(bind=db._saved_engine)


def drop_tables(session: Session):
    """
    Drops all the database tables. Very unsubtle. Use with caution

    Parameters
    ----------
    session : :class:`Session`
        Session to create tables in
    """
    File.metadata.drop_all(bind=db._saved_engine)
