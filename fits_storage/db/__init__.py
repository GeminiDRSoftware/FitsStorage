from contextlib import contextmanager
from datetime import date, datetime

import sqlalchemy
from sqlalchemy import create_engine, String, Date, DateTime
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.sqltypes import NullType

from fits_storage.config import get_config

_saved_engine = None
_saved_database_url = None
_saved_sessionfactory = None


def sessionfactory(reload=False):
    """
    Retrieves a singleton session factory.

    This call grants access to a singleton SQLAlchemy session factory.  If
    the factory does not exist yet, it is created from the 'database_url'
    fits storage config value

    Returns
    -------
    :class:`~sqlalchemy.orm.sessionmaker` SQLAlchemy session factory
    """
    global _saved_engine
    global _saved_database_url
    global _saved_sessionfactory

    fsc = get_config()

    if _saved_database_url is None or reload:
        _saved_database_url = fsc.database_url
        if _saved_database_url.startswith('postgresql://'):
            args = {'pool_size': fsc.postgres_database_pool_size,
                    'max_overflow': fsc.postgres_database_max_overflow,
                    'echo': fsc.database_debug}
        else:
            args = {'echo': fsc.database_debug}
        # TODO: SQLALCHEMY Remove future flag when upgraded to SQLAlchemy 2
        _saved_engine = create_engine(fsc.database_url, future=True, **args)
        _saved_sessionfactory = sessionmaker(_saved_engine)
    return _saved_sessionfactory()


Base = sqlalchemy.orm.declarative_base()


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
    return stmt.compile(dialect=_LiteralDialect(),
                        compile_kwargs={'literal_binds': True})
