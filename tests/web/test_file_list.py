import os

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
from fits_storage.utils.web import Return
from fits_storage.utils.web.wsgi_adapter import ItemizedFieldStorage
from fits_storage.web.api import lookup_diskfile
from fits_storage.web.file_list import xmlfilelist, diskfile_dicts, jsonfilelist, jsonsummary, jsonqastate
from fits_storage.web.miscfiles import miscfiles, detail_miscfile, string_to_date, validate
from fits_storage.web.progsobserved import progsobserved, sitemap
from fits_storage.web.publication import publication_ads, list_publications
from tests.file_helper import setup_mock_file_stuff
from tests.web_helper import MockContext


def make_test_file(session):
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

    return f, df, h, m


@pytest.mark.usefixtures("rollback")
def test_xmlfilelist(session, monkeypatch):
    setup_mock_file_stuff(monkeypatch)

    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.file_list, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.list_headers, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    f, df, h, m = make_test_file(session)

    xmlfilelist({})

    assert(mock_context.resp.status == 200)
    assert('<filename>foo.fits</filename>' in mock_context.resp.stuff)

    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_diskfile_dicts(session, monkeypatch):
    setup_mock_file_stuff(monkeypatch)

    f, df, h, m = make_test_file(session)

    ret = diskfile_dicts([h,])
    retval = [r for r in ret]

    assert(len(retval) == 1)
    retval = retval[0]
    assert(retval['name'] == 'foo.fits')
    assert(retval['filename'] == 'foo.fits')
    assert(retval['path'] == '')
    assert(retval['compressed'] is False)
    assert(retval['file_size'] == 0)
    assert(retval['data_size'] == 0)
    assert(retval['file_md5'] == '')
    assert(retval['data_md5'] == '')
    assert(retval['lastmod'] == df.lastmod.strftime('%Y-%m-%d %H:%M:%S.%f'))
    assert(retval['mdready'] is None)
    assert(retval['size'] == 0)

    ret = diskfile_dicts([h, ], return_header=True)
    retval = [r for r in ret]
    assert(len(retval) == 1)
    retval, hdr = retval[0]
    assert(hdr.id == h.id)
    assert(retval['name'] == 'foo.fits')
    assert(retval['filename'] == 'foo.fits')
    assert(retval['path'] == '')
    assert(retval['compressed'] is False)
    assert(retval['file_size'] == 0)
    assert(retval['data_size'] == 0)
    assert(retval['file_md5'] == '')
    assert(retval['data_md5'] == '')
    assert(retval['lastmod'] == df.lastmod.strftime('%Y-%m-%d %H:%M:%S.%f'))
    assert(retval['mdready'] is None)
    assert(retval['size'] == 0)

    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_jsonfilelist(session, monkeypatch):
    setup_mock_file_stuff(monkeypatch)

    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.file_list, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.list_headers, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    f, df, h, m = make_test_file(session)

    jsonfilelist({})

    assert(mock_context.resp.status == 200)
    assert(mock_context.resp.json_list is not None)
    assert(mock_context.resp.json_indent == 4)
    assert(len(mock_context.resp.json_list) == 1)

    js = mock_context.resp.json_list[0]
    assert(js['name'] == 'foo.fits')
    assert(js['filename'] == 'foo.fits')
    assert(js['path'] == '')
    assert(js['compressed'] is False)
    assert(js['file_size'] == 0)
    assert(js['data_size'] == 0)
    assert(js['file_md5'] == '')
    assert(js['mdready'] is None)
    assert(js['size'] == 0)
    assert(js['md5'] == '')

    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_jsonsummary(session, monkeypatch):
    setup_mock_file_stuff(monkeypatch)

    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context

    monkeypatch.setattr(fits_storage.web.file_list, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.list_headers, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    f, df, h, m = make_test_file(session)

    jsonsummary({})

    assert(mock_context.resp.status == 200)

    assert(len(mock_context.resp.json_list) == 1)

    js = mock_context.resp.json_list[0]

    assert(js['name'] == 'foo.fits')
    assert(js['filename'] == 'foo.fits')
    assert(js['path'] == '')
    assert(js['compressed'] is False)
    assert(js['file_size'] == 0)
    assert(js['data_size'] == 0)
    assert(js['file_md5'] == '')
    assert(js['mdready'] is None)
    assert(js['size'] == 0)
    assert(js['md5'] == '')

    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_jsonqastate(session, monkeypatch):
    setup_mock_file_stuff(monkeypatch)

    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context

    monkeypatch.setattr(fits_storage.web.file_list, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.list_headers, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    f, df, h, m = make_test_file(session)

    jsonqastate({})

    assert(mock_context.resp.status == 200)

    assert(len(mock_context.resp.json_list) == 1)

    js = mock_context.resp.json_list[0]

    assert(js['data_label'] is None)
    assert(js['filename'] == 'foo.fits')
    assert(js['data_md5'] == '')
    assert(js['qa_state'] is None)

    session.rollback()
