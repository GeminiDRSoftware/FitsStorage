import urllib

import pytest

import fits_storage
from fits_storage.orm.queue_error import QueueError
from fits_storage.orm.user import User
from fits_storage.orm.userprogram import UserProgram
from fits_storage.web.api import lookup_diskfile
from fits_storage.web.userprogram import my_programs, get_permissions_list, get_program_list, get_file_list, \
    request_user_program, get_obsid_list, validate_program_key
from tests.web_helper import MockContext, MockUser


@pytest.mark.usefixtures("rollback")
def test_my_programs(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.userprogram, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.user, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    my_programs('program_id=program/program_key=program_key')
    assert(mock_context.resp.status == 200)
    assert("Register a new program" in mock_context.resp.stuff)


@pytest.mark.usefixtures("rollback")
def test_get_permissions_list(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.userprogram, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.user, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    user = User('testuser')
    session.add(user)
    session.flush()

    session.add(UserProgram(user.id, 'program1'))
    session.add(UserProgram(user.id, 'program2'))
    session.add(UserProgram(user.id, None, 'obsid1'))
    session.add(UserProgram(user.id, None, 'obsid2'))
    session.add(UserProgram(user.id, None, None, 'file1', None))
    session.add(UserProgram(user.id, None, None, 'file2', 'path'))
    session.flush()

    prog_list, obsid_list, file_list = get_permissions_list(user)

    assert('program1' in prog_list)
    assert('program2' in prog_list)
    assert('obsid1' in obsid_list)
    assert('obsid2' in obsid_list)
    assert(('', 'file1') in file_list)
    assert(('path', 'file2') in file_list)


@pytest.mark.usefixtures("rollback")
def test_get_program_list(session, monkeypatch):

    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.userprogram, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.user, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    user = User('testuser')
    session.add(user)
    session.flush()

    session.add(UserProgram(user.id, 'program1'))
    session.add(UserProgram(user.id, 'program2'))
    session.add(UserProgram(user.id, None, 'obsid1'))
    session.add(UserProgram(user.id, None, 'obsid2'))
    session.add(UserProgram(user.id, None, None, 'file1', None))
    session.add(UserProgram(user.id, None, None, 'file2', 'path'))
    session.flush()

    prog_list = get_program_list(user)

    assert('program1' in prog_list)
    assert('program2' in prog_list)


@pytest.mark.usefixtures("rollback")
def test_get_obsid_list(session, monkeypatch):

    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.userprogram, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.user, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    user = User('testuser')
    session.add(user)
    session.flush()

    session.add(UserProgram(user.id, 'program1'))
    session.add(UserProgram(user.id, 'program2'))
    session.add(UserProgram(user.id, None, 'obsid1'))
    session.add(UserProgram(user.id, None, 'obsid2'))
    session.add(UserProgram(user.id, None, None, 'file1', None))
    session.add(UserProgram(user.id, None, None, 'file2', 'path'))
    session.flush()

    obsid_list = get_obsid_list(user)

    assert('obsid1' in obsid_list)
    assert('obsid2' in obsid_list)


@pytest.mark.usefixtures("rollback")
def test_get_file_list(session, monkeypatch):

    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.userprogram, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.user, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    user = User('testuser')
    session.add(user)
    session.flush()

    session.add(UserProgram(user.id, 'program1'))
    session.add(UserProgram(user.id, 'program2'))
    session.add(UserProgram(user.id, None, 'obsid1'))
    session.add(UserProgram(user.id, None, 'obsid2'))
    session.add(UserProgram(user.id, None, None, 'file1', None))
    session.add(UserProgram(user.id, None, None, 'file2', 'path'))
    session.flush()

    file_list = get_file_list(user)

    assert(('', 'file1') in file_list)
    assert(('path', 'file2') in file_list)


@pytest.mark.usefixtures("rollback")
def test_request_user_program(session, monkeypatch):

    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.userprogram, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.user, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    def _mock_validate_program_key(program_id, program_key):
        return True
    monkeypatch.setattr(fits_storage.web.userprogram, "validate_program_key", _mock_validate_program_key)

    user = User('testuser')
    session.add(user)
    session.flush()

    request_user_program(user, 'program_id', 'program_key')

    res = session.query(UserProgram).filter(UserProgram.user_id == user.id, UserProgram.program_id == 'program_id').one()
    assert(res is not None)


@pytest.mark.usefixtures("rollback")
def test_validate_program_key(session, monkeypatch):
    assert(validate_program_key('foo', 'bar') == False)

    saved_url = None

    class Ret:

        def read(self):
            return b'YES'

        def close(self):
            pass

    def _mock_urlopen(url):
        nonlocal saved_url
        saved_url = url
        return Ret()
    monkeypatch.setattr(urllib.request, 'urlopen', _mock_urlopen)

    assert(validate_program_key('GN001', 'key'))
    assert(saved_url == 'https://gnodb.gemini.edu:8443/auth?id=GN001&password=key')