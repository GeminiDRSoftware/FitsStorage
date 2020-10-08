import collections
from cgi import FieldStorage

from datetime import datetime, timedelta

import pytest

import fits_storage
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.file import File
from fits_storage.orm.header import Header
from fits_storage.orm.miscfile import MiscFile
from fits_storage.orm.program import Program
from fits_storage.orm.publication import Publication
from fits_storage.utils.web.wsgi_adapter import ItemizedFieldStorage
from fits_storage.web.api import lookup_diskfile
from fits_storage.web.miscfiles import miscfiles, detail_miscfile
from fits_storage.web.progsobserved import progsobserved, sitemap
from fits_storage.web.publication import publication_ads, list_publications
from tests.web_helper import MockContext


def mock_get_file_size(path):
    return 0


def mock_get_file_md5(path):
    return ''


def mock_get_lastmod(path):
    return datetime.now()


def mock_populate_fits(hdr, df, log):
    pass


@pytest.mark.usefixtures("rollback")
def test_miscfiles(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.miscfiles, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    miscfiles()
    assert(mock_context.resp.status == 200)
    assert('Generic Files Management' in mock_context.resp.stuff)

    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_miscfiles_search(session, monkeypatch):
    Field = collections.namedtuple('Field', 'name value')
    form_data = ItemizedFieldStorage()
    form_data.list = [
        Field(name='prog', value='program'),
        Field(name='search', value=True)
    ]

    mock_context = MockContext(session, method='GET', form_data=form_data)

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.miscfiles, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, 'get_file_size', mock_get_file_size)
    monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, 'get_file_md5', mock_get_file_md5)
    monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, 'get_lastmod', mock_get_lastmod)
    monkeypatch.setattr(fits_storage.orm.header.Header, 'populate_fits', mock_populate_fits)

    f = File('foo.fits')
    session.add(f)
    session.flush()
    df = DiskFile(f, 'foo.fits', '')
    session.add(df)
    session.flush()
    h = Header(df, None)
    h.ut_datetime = datetime(2020, 5, 1)
    h.program_id = 'program'
    session.add(h)
    session.flush()
    m = MiscFile()
    m.diskfile_id = df.id
    m.program_id = 'program'
    m.release = datetime.now() - timedelta(days=366)
    session.add(m)
    session.flush()

    miscfiles()
    assert(mock_context.resp.status == 200)
    assert('<td>program' in mock_context.resp.stuff)

    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_miscfiles_upload(session, monkeypatch):
    Field = collections.namedtuple('Field', 'name value')
    form_data = ItemizedFieldStorage()
    form_data.list = [
        Field(name='prog', value='program'),
        Field(name='upload', value=True)
    ]

    mock_context = MockContext(session, method='GET', form_data=form_data)

    def _mock_get_context(initialize=True):
        return mock_context

    save_called = False

    def _mock_save_file(formdata):
        nonlocal save_called
        save_called = True

    monkeypatch.setattr(fits_storage.web.miscfiles, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.miscfiles, "save_file", _mock_save_file)
    monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, 'get_file_size', mock_get_file_size)
    monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, 'get_file_md5', mock_get_file_md5)
    monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, 'get_lastmod', mock_get_lastmod)
    monkeypatch.setattr(fits_storage.orm.header.Header, 'populate_fits', mock_populate_fits)

    miscfiles()

    assert(mock_context.resp.status == 200)
    assert(save_called is True)


@pytest.mark.usefixtures("rollback")
def test_miscfiles_handle(session, monkeypatch):
    Field = collections.namedtuple('Field', 'name value')
    form_data = ItemizedFieldStorage()
    form_data.list = [
        Field(name='prog', value='program'),
        Field(name='search', value=True)
    ]

    mock_context = MockContext(session, method='GET', form_data=form_data)

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.miscfiles, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, 'get_file_size', mock_get_file_size)
    monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, 'get_file_md5', mock_get_file_md5)
    monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, 'get_lastmod', mock_get_lastmod)
    monkeypatch.setattr(fits_storage.orm.header.Header, 'populate_fits', mock_populate_fits)

    f = File('foo.fits')
    session.add(f)
    session.flush()
    df = DiskFile(f, 'foo.fits', '')
    session.add(df)
    session.flush()
    h = Header(df, None)
    h.ut_datetime = datetime(2020, 5, 1)
    h.program_id = 'program'
    session.add(h)
    session.flush()
    m = MiscFile()
    m.diskfile_id = df.id
    m.program_id = 'program'
    m.release = datetime.now() - timedelta(days=366)
    session.add(m)
    session.flush()

    detail_miscfile(handle=m.id)
    assert(mock_context.resp.status == 200)
    assert('<td>Program:<td>program' in mock_context.resp.stuff)

    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_miscfiles_handle_save(session, monkeypatch):

    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.miscfiles, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, 'get_file_size', mock_get_file_size)
    monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, 'get_file_md5', mock_get_file_md5)
    monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, 'get_lastmod', mock_get_lastmod)
    monkeypatch.setattr(fits_storage.orm.header.Header, 'populate_fits', mock_populate_fits)

    f = File('foo.fits')
    session.add(f)
    session.flush()
    df = DiskFile(f, 'foo.fits', '')
    session.add(df)
    session.flush()
    h = Header(df, None)
    h.ut_datetime = datetime(2020, 5, 1)
    h.program_id = 'program'
    session.add(h)
    session.flush()
    m = MiscFile()
    m.diskfile_id = df.id
    m.program_id = 'program'
    m.release = datetime.now() - timedelta(days=366)
    session.add(m)
    session.flush()

    Field = collections.namedtuple('Field', 'name value')
    form_data = ItemizedFieldStorage()
    form_data.list = [
        Field(name='save', value=True),
        Field(name='prog', value='newprog'),
        Field(name='desc', value='newdesc'),
        Field(name='release', value='default'),
    ]

    detail_miscfile(handle=m.id, formdata=form_data)
    assert(mock_context.resp.status == 200)
    assert('Successfully updated' in mock_context.resp.stuff)

    m = session.query(MiscFile).filter(MiscFile.id == m.id).first()
    assert(m.program_id == 'newprog')
    assert(m.description == 'newdesc')

    session.rollback()

