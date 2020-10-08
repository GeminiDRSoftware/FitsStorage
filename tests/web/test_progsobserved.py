from datetime import datetime

import pytest

import fits_storage
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.file import File
from fits_storage.orm.header import Header
from fits_storage.orm.program import Program
from fits_storage.orm.publication import Publication
from fits_storage.web.api import lookup_diskfile
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
def test_progsobserved(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.progsobserved, "get_context", _mock_get_context)
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
    p = Program('program')
    session.add(p)
    session.flush()

    progsobserved({"daterange": "20200101-20201230"})
    assert(mock_context.resp.status == 200)  # bibcode is invalid
    # assert('<p>program </p>' in mock_context.resp.stuff)
    assert('<li><a href="/summary/program/20200101-20201230">program</a></li>' in mock_context.resp.stuff)

    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_sitemap(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.progsobserved, "get_context", _mock_get_context)
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
    p = Program('program')
    session.add(p)
    session.flush()

    sitemap(mock_context.req)

    assert(mock_context.resp.status == 200)  # bibcode is invalid
    # assert('<p>program </p>' in mock_context.resp.stuff)
    assert('<lastmod>2015-09-01</lastmod>' in mock_context.resp.stuff)

    session.rollback()
