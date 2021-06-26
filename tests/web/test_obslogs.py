import datetime

import pytest

import fits_storage
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.file import File
from gemini_obs_db.header import Header
from fits_storage.orm.obslog import Obslog
from fits_storage.orm.querylog import QueryLog
from fits_storage.orm.usagelog import UsageLog
from fits_storage.web.list_headers import list_headers
from fits_storage.web.obslogs import add_summary_completed, generate_obslogs, obslogs
from tests.web_helper import MockContext


@pytest.mark.usefixtures("rollback")
def test_add_summary_completed(session, monkeypatch):
    try:
        mock_context = MockContext(session, method='GET')

        ul = UsageLog(mock_context)
        mock_context.usagelog = ul
        session.add(ul)
        session.flush()
        ql = QueryLog(ul)
        session.add(ql)
        session.flush()

        qlid = ql.id

        def _mock_get_context(initialize=True):
            return mock_context

        monkeypatch.setattr(fits_storage.web.obslogs, "get_context", _mock_get_context)

        assert(ql.summary_completed is None)
        add_summary_completed()
        assert(ql.summary_completed is not None)
    finally:
        session.rollback()


def test_generate_obslogs(session, monkeypatch):
    try:
        mock_context = MockContext(session, method='GET')

        def _mock_get_context(initialize=True):
            return mock_context

        def _mock_diskfile_get_file_size(df):
            return 0

        def _mock_diskfile_get_file_md5(df):
            return ''

        def _mock_diskfile_get_lastmod(df):
            return datetime.datetime.utcnow()

        def _mock_icanhave(ctx, obslog):
            return True

        monkeypatch.setattr(fits_storage.web.obslogs, "get_context", _mock_get_context)
        monkeypatch.setattr(fits_storage.web.obslogs, "icanhave", _mock_icanhave)
        monkeypatch.setattr(fits_storage.web.userprogram, "get_context", _mock_get_context)
        monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, "get_file_size", _mock_diskfile_get_file_size)
        monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, "get_file_md5", _mock_diskfile_get_file_md5)
        monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, "get_lastmod", _mock_diskfile_get_lastmod)

        f = File('somefile.fits')
        session.add(f)
        session.flush()
        df = DiskFile(f, given_filename='somefile.fits', path='')
        session.add(df)
        session.flush()
        ol = Obslog(df)
        session.add(ol)
        session.flush()

        seen = False

        for r in generate_obslogs([ol, ]):
            seen = True

        assert(seen)
    finally:
        session.rollback()


def test_obslogs(session, monkeypatch):
    try:
        mock_context = MockContext(session, method='GET')
        ul = UsageLog(mock_context)
        mock_context.usagelog = ul
        session.add(ul)
        session.flush()
        mock_context.usagelog = ul

        def _mock_get_context(initialize=True):
            return mock_context

        def _mock_diskfile_get_file_size(df):
            return 0

        def _mock_diskfile_get_file_md5(df):
            return ''

        def _mock_diskfile_get_lastmod(df):
            return datetime.datetime.utcnow()

        def _mock_icanhave(ctx, obslog):
            return True

        monkeypatch.setattr(fits_storage.web.obslogs, "get_context", _mock_get_context)
        monkeypatch.setattr(fits_storage.web.obslogs, "icanhave", _mock_icanhave)
        monkeypatch.setattr(fits_storage.web.userprogram, "get_context", _mock_get_context)
        monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)
        monkeypatch.setattr(fits_storage.web.list_headers, "get_context", _mock_get_context)
        monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, "get_file_size", _mock_diskfile_get_file_size)
        monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, "get_file_md5", _mock_diskfile_get_file_md5)
        monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, "get_lastmod", _mock_diskfile_get_lastmod)

        f = File('somefile.fits')
        session.add(f)
        session.flush()
        df = DiskFile(f, given_filename='somefile.fits', path='')
        session.add(df)
        session.flush()
        ol = Obslog(df)
        session.add(ol)
        session.flush()

        r = obslogs({'date': '20200101', }, '')

        assert('DOCTYPE' in mock_context.resp.stuff)
        assert(mock_context.resp.status == 200)
    finally:
        session.rollback()


def test_associate_obslogs(session, monkeypatch):
    try:
        mock_context = MockContext(session, method='GET')
        ul = UsageLog(mock_context)
        mock_context.usagelog = ul
        session.add(ul)
        session.flush()
        mock_context.usagelog = ul

        def _mock_get_context(initialize=True):
            return mock_context

        def _mock_diskfile_get_file_size(df):
            return 0

        def _mock_diskfile_get_file_md5(df):
            return ''

        def _mock_diskfile_get_lastmod(df):
            return datetime.datetime.utcnow()

        def _mock_icanhave(ctx, obslog):
            return True

        monkeypatch.setattr(fits_storage.web.obslogs, "get_context", _mock_get_context)
        monkeypatch.setattr(fits_storage.web.obslogs, "icanhave", _mock_icanhave)
        monkeypatch.setattr(fits_storage.web.userprogram, "get_context", _mock_get_context)
        monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)
        monkeypatch.setattr(fits_storage.web.list_headers, "get_context", _mock_get_context)
        monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, "get_file_size", _mock_diskfile_get_file_size)
        monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, "get_file_md5", _mock_diskfile_get_file_md5)
        monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, "get_lastmod", _mock_diskfile_get_lastmod)

        f = File('somefile.fits')
        session.add(f)
        session.flush()
        df = DiskFile(f, given_filename='somefile.fits', path='')
        session.add(df)
        session.flush()
        ol = Obslog(df)
        session.add(ol)
        session.flush()

        r = obslogs({'date': '20200101', }, '')

        assert('DOCTYPE' in mock_context.resp.stuff)
        assert(mock_context.resp.status == 200)
    finally:
        session.rollback()


