from contextlib import contextmanager
from datetime import date, datetime

from sqlalchemy import create_engine, String, Date, DateTime
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.sqltypes import NullType

# from gemini_obs_db.db_config import database_url, postgres_database_pool_size, postgres_database_max_overflow
from gemini_obs_db import db_config as dbc

if dbc.database_url.startswith('postgresql://'):
    args = {'pool_size': dbc.postgres_database_pool_size, 'max_overflow': dbc.postgres_database_max_overflow,
            'echo': dbc.database_debug, 'pool_pre_ping': True}
else:
    args = {'echo': dbc.database_debug}
pg_db = create_engine(dbc.database_url, **args)
sessionfactory = sessionmaker(pg_db)


_saved_database_url = None
_saved_sessionfactory = None


def sessionfactory():
    """
    Retrieves a singleton session factory.

    This call grants access to a singleton SQLAlchemy session factory.  If the factory
    does not exist yet, it is created from :field:`~gemini_obs_db.db_config.database_url`.

    Returns
    -------
    :class:`~sqlalchemy.orm.sessionmaker` SQLAlchemy session factory
    """
    global pg_db
    global _saved_database_url
    global _saved_sessionfactory
    if _saved_database_url != dbc.database_url:
        _saved_database_url = dbc.database_url
        if dbc.database_url.startswith('postgresql://'):
            args = {'pool_size': dbc.postgres_database_pool_size, 'max_overflow': dbc.postgres_database_max_overflow,
                    'echo': dbc.database_debug}
        else:
            args = {'echo': dbc.database_debug}
        pg_db = create_engine(dbc.database_url, **args)
        _saved_sessionfactory = sessionmaker(pg_db)
    return _saved_sessionfactory()


Base = declarative_base()


@contextmanager
def session_scope(no_rollback=False):
    """
    Provide a transactional scope around a series of operations

    Parameters
    ----------
    no_rollback: bool
        True if we want to always commit, default is False

    Returns
    -------
    :class:`~sqlalchemy.orm.Session`
        Session with automatic commit/rollback handling on leaving the context
    """
    session = sessionfactory()
    try:
        yield session
        session.commit()
    except:
        if not no_rollback:
            session.rollback()
        else:
            session.commit()
        raise
    finally:
        session.close()


class _StringLiteral(String):
    """
    Literal used for a custom SQLAlchemy dialect for debugging.

    To debug SQLAlchemy queries, it is useful to have this custom
    dialect that will convert the query, not into SQL, but into
    a readable text string.  This class helps with that.
    """
    def literal_processor(self, dialect):
        super_processor = super(_StringLiteral, self).literal_processor(dialect)

        def process(value):
            if isinstance(value, (date, datetime)) or value is None:
                return str(value)
            return super_processor(value)
        return process


class _LiteralDialect(postgresql.dialect):
    """
    Literal used for a custom SQLAlchemy dialect for debugging.

    To debug SQLAlchemy queries, it is useful to have this custom
    dialect that will convert the query, not into SQL, but into
    a readable text string.  This class is the top level dialect
    description for that purpose.
    """
    colspecs = {
        Date: _StringLiteral,
        DateTime: _StringLiteral,
        NullType: _StringLiteral
    }


def compiled_statement(stmt):
    """
    Returns a compiled query using the PostgreSQL dialect. Useful for
    example to print the real query, when debugging

    Parameters
    ----------
    stmt : :class:`~sqlalchemy.orm.statement.Statement`

    Returns
    -------
    str
        String representation of the query
    """
    return stmt.compile(dialect=_LiteralDialect(), compile_kwargs={'literal_binds': True})
