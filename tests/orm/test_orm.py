from __future__ import print_function

import pytest
# from FitsStorage import fits_storage_config as fsc
import fits_storage.fits_storage_config as fsc
import datetime as dt
import sqlalchemy.orm.exc as orm_exc
from decorator import decorator

now = dt.datetime.now()
# Monkeypatch the database name before doing anything...
# We'll use the current date and time to generate new databases. We don't expect
# tests to last under a second, so this should be safe... (otherwise, something is
# really, really wrong)
fsc.fits_dbname = 'test_{0}_{1}'.format(fsc.fits_dbname, now.strftime('%Y%m%d%H%M%S'))
fsc.fits_database = 'postgresql:///' + fsc.fits_dbname

import sqlalchemy
from fits_storage import orm
from fits_storage.orm import createtables
from fits_storage.orm.user import User

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

@pytest.yield_fixture()
def rollback(request, session):
    '''This will be used from most other tests, to make sure that a
       database failure won't interfere with other functions, and that
       unintended changes don't get passed to other tests'''
    yield session
    session.rollback()

@pytest.fixture(scope='session')
def user(request, session):
    try:
        user = session.query(User).filter(User.username == 'testuser').one()
    except orm_exc.NoResultFound:
        user = User('testuser')
        session.add(user)
        session.commit()

    return user

@pytest.mark.usefixtures("rollback")
class TestUser:
    def test_reset_and_validate_password(self, session, user):
        assert user.password is None  # For a just created user, this should be null
        user.reset_password('foobar')
        assert user.validate_password('foobar') == True

    def test_generate_and_validate_reset_token(self, session, user):
        assert user.reset_token is None
        token = user.generate_reset_token()
        assert user.validate_reset_token('foo') == False
        assert user.validate_reset_token(None) == False
        assert user.validate_reset_token(token) == True
        assert (user.reset_token is None and user.reset_token_expires is None) == True

    def test_log_in(self, session, user):
        # Test that we start with an empty cookie
        assert user.cookie is None
        # Test that a new cookie has been generated after log in
        user.log_in()
        assert user.cookie is not None
        # Make sure that a second log in keeps the existing cookie
        cookie = user.cookie
        user.log_in()
        assert user.cookie == cookie

    def test_log_out(self, session, user):
        user.log_in()
        # Test that we start with a logged in user
        assert user.cookie is not None
        # Test that logging out invalidates the existing cookie
        user.log_out_all()
        assert user.cookie is None
