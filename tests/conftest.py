import pytest
import datetime as dt
import os
from subprocess import call
from bz2 import BZ2File
import sys
now = dt.datetime.now()

RESTORE='/usr/bin/pg_restore'
DUMP_FILE='fitsdata_test.pg_dump'
full_path_dump=os.path.join(os.path.dirname(__file__), DUMP_FILE)

# Monkeypatch the database name and a few other things before doing anything...
# We'll use the current date and time to generate new databases. We don't expect
# tests to last under a second, so this should be safe... (otherwise, something is
# really, really wrong)

import fits_storage.fits_storage_config as fsc
fsc.fits_dbname = 'test_{0}_{1}'.format(fsc.fits_dbname, now.strftime('%Y%m%d%H%M%S'))
fits_dbserver = os.getenv('FITS_DB_SERVER', '')
fsc.fits_database = 'postgresql://%s/%s' % (fits_dbserver, fsc.fits_dbname)
fsc.using_s3 = False
fsc.pytest_database_server = os.getenv("FITS_DB_SERVER", '')

TEST_IMAGE_PATH='/mnt/hahalua'
TEST_IMAGE_CACHE=os.path.expanduser('~/tmp/cache')

import sqlalchemy
from fits_storage import orm
from fits_storage.orm import createtables

class DatabaseCreation(object):
    def __init__(self):
        self.conn = None

    def create_db(self, dbname):
        if self.conn is None:

            eng = sqlalchemy.create_engine('postgres://%s/postgres' % fsc.pytest_database_server)
            conn = eng.connect()
            conn.execute('COMMIT') # Make sure we're not inside a transaction
                                   # as CREATE DATABASE can't run inside one
            conn.execute('CREATE DATABASE ' + dbname)

            # Trying to fix test_wsgi.py
            conn.close()
            eng = sqlalchemy.create_engine('postgres://%s/%s' % (fsc.pytest_database_server, dbname))
            conn = eng.connect()
            # end of my hackery

            self.conn = conn
        else:
            conn = self.conn
        s = orm.sessionfactory()

        return conn, s

    def drop_db(self, dbname):
        if self.conn:
            conn = self.conn
            #O orm.pg_db.dispose()

            conn.execute('COMMIT')
            # Kill any other pending connection. Shouldn't be needed, but...
            #O conn.execute("SELECT pg_terminate_backend(procpid) FROM pg_stat_activity WHERE datname = '%s' AND procpid <> pg_backend_pid()" % (fsc.fits_dbname,))
            conn.execute('DROP DATABASE ' + dbname)
            conn.close()
            self.conn = None

            #O moved down here from above
            orm.pg_db.dispose()

dbcreation = DatabaseCreation()

@pytest.yield_fixture(scope='session')
def session(request):
    'Creates a fresh database, with empty tables'
    dbname = fsc.fits_dbname
    conn, s = dbcreation.create_db(dbname)
    orm.createtables.create_tables(s)

    yield s

    dbcreation.drop_db(dbname)

@pytest.yield_fixture(scope='session')
def min_session(request):
    'Creates a fresh database, with empty tables'
    dbname = fsc.fits_dbname
    conn, s = dbcreation.create_db(dbname)
    #call([RESTORE, '-d', dbname, DUMP_FILE])
    if fsc.pytest_database_server != '':
        # Need to strip off
        hostname = fsc.pytest_database_server
        if '@' in hostname:
            hostname = hostname[hostname.index('@')+1:]
        # Note, password comes from env var PGPASSWORD, which is set in Dockerfile (or add it to your env to suit your needs)
        call([RESTORE, '-h', hostname, '--username', 'fitsdata', '-d', dbname, full_path_dump])
    else:
        call([RESTORE, '-d', dbname, full_path_dump])

    yield s

    dbcreation.drop_db(dbname)

@pytest.yield_fixture(scope='session')
def rollback(request, session):
    '''This will be used from most other tests, to make sure that a
       database failure won't interfere with other functions, and that
       unintended changes don't get passed to other tests'''
    yield session
    session.rollback()

@pytest.yield_fixture(scope='session')
def min_rollback(request, min_session):
    '''This will be used from most other tests, to make sure that a
       database failure won't interfere with other functions, and that
       unintended changes don't get passed to other tests'''
    yield min_session
    min_session.rollback()

@pytest.yield_fixture(scope='session')
def testfile_path(request):
    added_to_cache = []
    def return_image_path(filename):
        if filename.endswith('.bz2'):
            nobz2 = filename[:-4]
        else:
            nobz2 = filename
        cached_path = os.path.join(TEST_IMAGE_CACHE, nobz2)
        if not os.path.exists(cached_path):
            orig_path = os.path.join(TEST_IMAGE_PATH, filename)
            if not orig_path.endswith('.bz2'):
                os.symlink(orig_path, cached_path)
            else:
                with BZ2File(orig_path) as org, open(cached_path, 'w') as dst:
                    while True:
                        data = org.read(8192)
                        if not data:
                            break
                        dst.write(data)

            added_to_cache.append(cached_path)

        return cached_path

    yield return_image_path

    for cached in added_to_cache:
        os.unlink(cached)

# Slow test handling
#
# This section sets up behavior to avoid slow tests by default.

def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        skip_slow = pytest.mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

# End Slow test handling