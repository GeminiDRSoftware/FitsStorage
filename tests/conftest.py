import pytest
import datetime as dt
now = dt.datetime.now()

# Monkeypatch the database name before doing anything...
# We'll use the current date and time to generate new databases. We don't expect
# tests to last under a second, so this should be safe... (otherwise, something is
# really, really wrong)

import fits_storage.fits_storage_config as fsc
fsc.fits_dbname = 'test_{0}_{1}'.format(fsc.fits_dbname, now.strftime('%Y%m%d%H%M%S'))
fsc.fits_database = 'postgresql:///' + fsc.fits_dbname

import sqlalchemy
from fits_storage import orm
from fits_storage.orm import createtables

@pytest.yield_fixture(scope='session')
def session(request):
    'Creates a fresh database, with empty tables'
    eng = sqlalchemy.create_engine('postgres:///postgres')
    conn = eng.connect()
    conn.execute('COMMIT') # Make sure we're not inside a transaction
                           # as CREATE DATABASE can't run inside one
    conn.execute('CREATE DATABASE ' + fsc.fits_dbname)
    s = orm.sessionfactory()
    orm.createtables.create_tables(s)

    yield s

    orm.pg_db.dispose()

    conn.execute('COMMIT')
    conn.execute('DROP DATABASE ' + fsc.fits_dbname)
    conn.close()

@pytest.yield_fixture(scope='session')
def rollback(request, session):
    '''This will be used from most other tests, to make sure that a
       database failure won't interfere with other functions, and that
       unintended changes don't get passed to other tests'''
    yield session
    session.rollback()

