"""
This module contains the ORM classes for the tables in the fits storage
database.
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql
from sqlalchemy import func, update
from sqlalchemy.sql.sqltypes import String, Date, DateTime, NullType
from datetime import datetime, date
from contextlib import contextmanager

from ..fits_storage_config import fits_database, fits_database_pool_size, fits_database_max_overflow

# This was to debug the number of open database sessions.
#import logging
#logging.basicConfig(filename='/data/autoingest/debug.log', level=logging.DEBUG)
#logging.getLogger('sqlalchemy.pool').setLevel(logging.INFO)


# Base = declarative_base()

# Create a database engine connection to the postgres database
# and an sqlalchemy session to go with it
# pg_db = create_engine(fits_database, pool_size=fits_database_pool_size,
#                       max_overflow=fits_database_max_overflow, echo=False)
# sessionfactory = sessionmaker(pg_db)
#
# @contextmanager
# def session_scope(no_rollback=False):
#     "Provide a transactional scope around a series of operations"
#
#     session = sessionfactory()
#     try:
#         yield session
#         session.commit()
#     except:
#         if not no_rollback:
#             session.rollback()
#         else:
#             session.commit()
#         raise
#     finally:
#         session.close()
#
# class StringLiteral(String):
#     def literal_processor(self, dialect):
#         super_processor = super(StringLiteral, self).literal_processor(dialect)
#         def process(value):
#             if isinstance(value, (date, datetime)) or value is None:
#                 return str(value)
#             return super_processor(value)
#         return process
#
# class LiteralDialect(postgresql.dialect):
#     colspecs = {
#         Date: StringLiteral,
#         DateTime: StringLiteral,
#         NullType: StringLiteral
#     }
#
# def compiled_statement(stmt):
#     """Returns a compiled query using the PostgreSQL dialect. Useful for
#        example to print the real query, when debugging"""
#     return stmt.compile(dialect = LiteralDialect(), compile_kwargs={'literal_binds': True})
#
