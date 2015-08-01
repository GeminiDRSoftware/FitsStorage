import pytest
import datetime as dt
import os
from bz2 import BZ2File
import sys
now = dt.datetime.now()

# Monkeypatch the database name and a few other things before doing anything...
# We'll use the current date and time to generate new databases. We don't expect
# tests to last under a second, so this should be safe... (otherwise, something is
# really, really wrong)

import fits_storage.fits_storage_config as fsc
fsc.fits_dbname = 'test_{0}_{1}'.format(fsc.fits_dbname, now.strftime('%Y%m%d%H%M%S'))
fsc.fits_database = 'postgresql:///' + fsc.fits_dbname
fsc.using_s3 = False

TEST_IMAGE_PATH='/mnt/hahalua'
TEST_IMAGE_CACHE=os.path.expanduser('~/tmp/cache')

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
