"""
This module contains the ORM classes for the tables in the fits storage
database.
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from fits_storage_config import fits_database

# This was to debug the number of open database sessions.
#import logging
#logging.basicConfig(filename='/data/autoingest/debug.log', level=logging.DEBUG)
#logging.getLogger('sqlalchemy.pool').setLevel(logging.INFO)


Base = declarative_base()

# Create a database engine connection to the postgres database
# and an sqlalchemy session to go with it
pg_db = create_engine(fits_database, echo = False)
sessionfactory = sessionmaker(pg_db)
