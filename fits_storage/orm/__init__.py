"""
This module contains the ORM classes for the tables in the fits storage
database.
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql
from sqlalchemy import func, update
from sqlalchemy.sql.sqltypes import String, Date, DateTime, NullType
from datetime import datetime, date

from ..fits_storage_config import fits_database

# This was to debug the number of open database sessions.
#import logging
#logging.basicConfig(filename='/data/autoingest/debug.log', level=logging.DEBUG)
#logging.getLogger('sqlalchemy.pool').setLevel(logging.INFO)


Base = declarative_base()

# Create a database engine connection to the postgres database
# and an sqlalchemy session to go with it
pg_db = create_engine(fits_database, echo = False)
sessionfactory = sessionmaker(pg_db)

class StringLiteral(String):
    def literal_processor(self, dialect):
        super_processor = super(StringLiteral, self).literal_processor(dialect)
        def process(value):
            if isinstance(value, (date, datetime)) or value is None:
                return str(value)
            return super_processor(value)
        return process

class LiteralDialect(postgresql.dialect):
    colspecs = {
        Date: StringLiteral,
        DateTime: StringLiteral,
        NullType: StringLiteral
    }

def compiled_statement(stmt):
    """Returns a compiled query using the PostgreSQL dialect. Useful for
       example to print the real query, when debugging"""
    return stmt.compile(dialect = LiteralDialect(), compile_kwargs={'literal_binds': True})

