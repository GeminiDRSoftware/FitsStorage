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

from ..fits_storage_config import fits_database

Base = declarative_base()

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
