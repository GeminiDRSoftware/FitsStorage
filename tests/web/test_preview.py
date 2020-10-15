from datetime import datetime

import pytest

import fits_storage
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.diskfilereport import DiskFileReport
from fits_storage.orm.file import File
from fits_storage.orm.fulltextheader import FullTextHeader
from fits_storage.orm.header import Header
from fits_storage.orm.preview import Preview
from fits_storage.orm.provenance import Provenance, ProvenanceHistory
from fits_storage.web.preview import num_previews
from fits_storage.web.reporting import report
from tests.file_helper import setup_mock_file_stuff
from tests.web_helper import MockContext, MockUsageLog


@pytest.mark.usefixtures("rollback")
def test_num_previews(session, monkeypatch):
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
    p = Preview(df, 'foo_preview.jpg')
    session.add(p)
    session.flush()

    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context

    monkeypatch.setattr(fits_storage.web.preview, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    num_previews('foo.fits')
    assert(mock_context.resp.status == 200)
    assert('1' == mock_context.resp.stuff)

    session.rollback()
