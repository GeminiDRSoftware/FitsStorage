
from datetime import datetime, timedelta

import pytest

import fits_storage
from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.file import File
from gemini_obs_db.orm.header import Header
from fits_storage.orm.miscfile import MiscFile
from fits_storage.web.api import lookup_diskfile
from fits_storage.web.file_list import xmlfilelist, diskfile_dicts, jsonfilelist, jsonsummary, jsonqastate
from tests.file_helper import setup_mock_file_stuff
from tests.web_helper import MockContext


def make_test_file(session):
    f = session.query(File).filter(File.name == 'tflfoo.fits').one_or_none()
    if f is None:
        f = File('tflfoo.fits')
        session.add(f)
        session.flush()
    df = session.query(DiskFile).filter(DiskFile.filename == 'tflfoo.fits').one_or_none()
    if df is None:
        df = DiskFile(f, 'tflfoo.fits', '')
        session.add(df)
        session.flush()
    h = session.query(Header).filter(Header.diskfile_id == df.id).one_or_none()
    if h is None:
        h = Header(df, None)
        h.ut_datetime = datetime(2020, 5, 1)
        h.program_id = 'tflprogram'
        session.add(h)
        session.flush()
    m = MiscFile()
    m.diskfile_id = df.id
    m.program_id = 'tflprogram'
    m.release = datetime.now() - timedelta(days=366)
    session.add(m)
    session.flush()

    return f, df, h, m


def cleanup_test_file(session, f, df, h, m):
    session.delete(m)
    session.delete(h)
    session.delete(df)
    session.delete(f)
    session.flush()


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

    xmlfilelist({'filename': 'tflfoo.fits'})

    assert(mock_context.resp.status == 200)
    assert('<filename>tflfoo.fits</filename>' in mock_context.resp.stuff)

    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_diskfile_dicts(session, monkeypatch):
    setup_mock_file_stuff(monkeypatch)

    f, df, h, m = make_test_file(session)

    ret = diskfile_dicts([h,])
    retval = [r for r in ret]

    assert(len(retval) == 1)
    retval = retval[0]
    assert(retval['name'] == 'tflfoo.fits')
    assert(retval['filename'] == 'tflfoo.fits')
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
    assert(retval['name'] == 'tflfoo.fits')
    assert(retval['filename'] == 'tflfoo.fits')
    assert(retval['path'] == '')
    assert(retval['compressed'] is False)
    assert(retval['file_size'] == 0)
    assert(retval['data_size'] == 0)
    assert(retval['file_md5'] == '')
    assert(retval['data_md5'] == '')
    assert(retval['lastmod'] == df.lastmod.strftime('%Y-%m-%d %H:%M:%S.%f'))
    assert(retval['mdready'] is None)
    assert(retval['size'] == 0)

    cleanup_test_file(session, f, df, h, m)

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

    jsonfilelist({'filename': 'tflfoo.fits'})

    assert(mock_context.resp.status == 200)
    assert(mock_context.resp.json_list is not None)
    assert(mock_context.resp.json_indent == 4)
    assert(len(mock_context.resp.json_list) == 1)

    js = mock_context.resp.json_list[0]
    assert(js['name'] == 'tflfoo.fits')
    assert(js['filename'] == 'tflfoo.fits')
    assert(js['path'] == '')
    assert(js['compressed'] is False)
    assert(js['file_size'] == 0)
    assert(js['data_size'] == 0)
    assert(js['file_md5'] == '')
    assert(js['mdready'] is None)
    assert(js['size'] == 0)
    assert(js['md5'] == '')

    cleanup_test_file(session, f, df, h, m)

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

    jsonsummary({'filename': 'tflfoo.fits'})

    assert(mock_context.resp.status == 200)

    assert(len(mock_context.resp.json_list) == 1)

    js = mock_context.resp.json_list[0]

    assert(js['name'] == 'tflfoo.fits')
    assert(js['filename'] == 'tflfoo.fits')
    assert(js['path'] == '')
    assert(js['compressed'] is False)
    assert(js['file_size'] == 0)
    assert(js['data_size'] == 0)
    assert(js['file_md5'] == '')
    assert(js['mdready'] is None)
    assert(js['size'] == 0)
    assert(js['md5'] == '')

    cleanup_test_file(session, f, df, h, m)

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

    jsonqastate({'filename': 'tflfoo.fits'})

    assert(mock_context.resp.status == 200)

    assert(len(mock_context.resp.json_list) == 1)

    js = mock_context.resp.json_list[0]

    assert(js['data_label'] is None)
    assert(js['filename'] == 'tflfoo.fits')
    assert(js['data_md5'] == '')
    assert(js['qa_state'] is None)

    cleanup_test_file(session, f, df, h, m)

    session.rollback()
