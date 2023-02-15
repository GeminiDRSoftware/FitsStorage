"""
This module provides various utility functions for create_tables.py
in the Fits Storage System.
"""
from sqlalchemy.orm import Session

import fits_storage.db as db

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
    Header.metadata.create_all(bind=db._saved_engine)
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
