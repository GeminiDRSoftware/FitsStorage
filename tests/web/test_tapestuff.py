from datetime import datetime

import pytest

import fits_storage
from fits_storage.orm.queue_error import QueueError
from fits_storage.orm.tapestuff import Tape, TapeFile, TapeWrite, TapeRead
from fits_storage.web.api import lookup_diskfile
from fits_storage.web.publication import publication_ads
from fits_storage.web.queuestatus import queuestatus_summary, queuestatus_tb, queuestatus_update
from fits_storage.web.tapestuff import fileontape, tapewrite, tapefile, taperead
from tests.web_helper import MockContext
from sqlalchemy import join, desc, func


@pytest.mark.usefixtures("rollback")
def test_fileontape(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.user, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.tapestuff, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    t = Tape('label')
    t.active = True
    session.add(t)
    session.flush()
    tape_write = TapeWrite()
    tape_write.tape_id = t.id
    tape_write.suceeded = True
    session.add(tape_write)
    session.flush()
    tape_file = TapeFile()
    tape_file.tapewrite_id = tape_write.id
    tape_file.filename = 'dummyfile.fits'
    tape_write.tapefiles.append(tape_file)
    session.add(tape_file)
    session.flush()

    fileontape('dummyfile.fits')

    assert(mock_context.resp.status == 200)
    assert('<filename>dummyfile.fits</filename>' in mock_context.resp.stuff)


@pytest.mark.usefixtures("rollback")
def test_tapewrite(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.user, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.tapestuff, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    t = Tape('label')
    t.active = True
    session.add(t)
    session.flush()
    tape_write = TapeWrite()
    tape_write.tape_id = t.id
    tape_write.suceeded = True
    session.add(tape_write)
    session.flush()
    tape_file = TapeFile()
    tape_file.tapewrite_id = tape_write.id
    tape_file.filename = 'dummyfile.fits'
    tape_write.tapefiles.append(tape_file)
    session.add(tape_file)
    session.flush()

    tapewrite(tape_write.id)

    assert(mock_context.resp.status == 200)
    assert('Tape Label: label' in mock_context.resp.stuff)


@pytest.mark.usefixtures("rollback")
def test_tapefile(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.user, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.tapestuff, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    t = Tape('label')
    t.active = True
    session.add(t)
    session.flush()
    tape_write = TapeWrite()
    tape_write.tape_id = t.id
    tape_write.suceeded = True
    session.add(tape_write)
    session.flush()
    tape_file = TapeFile()
    tape_file.tapewrite_id = tape_write.id
    tape_file.filename = 'dummyfile.fits'
    tape_write.tapefiles.append(tape_file)
    session.add(tape_file)
    session.flush()

    tapefile(tape_file.id)

    assert(mock_context.resp.status == 200)
    assert('<td>dummyfile.fits' in mock_context.resp.stuff)


@pytest.mark.usefixtures("rollback")
def test_taperead(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.user, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.tapestuff, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    tape_read = TapeRead()
    tape_read.filename = 'dummyfile.fits'
    tape_read.md5 = 'md5'
    tape_read.requester = 'requester'
    session.add(tape_read)
    session.flush()

    trs = session.query(TapeRead).all()

    taperead()

    assert(mock_context.resp.status == 200)
    assert('<td>dummyfile.fits' in mock_context.resp.stuff)
