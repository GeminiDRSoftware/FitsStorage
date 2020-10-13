from datetime import datetime

import pytest

import fits_storage
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.file import File
from fits_storage.orm.header import Header
from fits_storage.web.statistics import stats, content
from tests.file_helper import setup_mock_file_stuff
from tests.web_helper import MockContext


@pytest.mark.usefixtures("rollback")
def test_stats(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context

    # monkeypatch.setattr(fits_storage.web.user, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.statistics, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    stats()

    assert(mock_context.resp.status == 200)
    assert('database statistics' in mock_context.resp.stuff)


@pytest.mark.usefixtures("rollback")
def test_content(session, monkeypatch):
    setup_mock_file_stuff(monkeypatch)

    f = File('foo.fits')
    session.add(f)
    session.flush()
    df = DiskFile(f, 'foo.fits', '')
    df.canonical = True
    session.add(df)
    session.flush()
    h = Header(df)
    h.ut_datetime = datetime.now()
    session.add(h)
    session.flush()

    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context

    monkeypatch.setattr(fits_storage.web.statistics, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    content()

    assert(mock_context.resp.status == 200)
    assert('content statistics' in mock_context.resp.stuff)

    session.rollback()

