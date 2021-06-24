from datetime import datetime

import pytest

import fits_storage
from gemini_obs_db.diskfile import DiskFile
from fits_storage.orm.diskfilereport import DiskFileReport
from gemini_obs_db.file import File
from fits_storage.orm.fulltextheader import FullTextHeader
from gemini_obs_db.header import Header
from fits_storage.orm.provenance import Provenance, ProvenanceHistory
from fits_storage.web.reporting import report
from tests.file_helper import setup_mock_file_stuff
from tests.web_helper import MockContext, MockUsageLog


@pytest.mark.usefixtures("rollback")
def test_report(session, monkeypatch):
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
    ul = MockUsageLog()
    ul.this = 'fullheader'
    mock_context.usagelog =ul

    def _mock_get_context(initialize=True):
        return mock_context

    monkeypatch.setattr(fits_storage.web.reporting, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    report('%s' % df.id)

    assert(mock_context.resp.status == 200)
    assert('Cannot find report for' in mock_context.resp.stuff)

    dfr = DiskFileReport(df, True, True)
    session.add(dfr)
    session.flush()
    fth = FullTextHeader(df)
    fth.fulltext = 'This is the full text of the header'
    session.add(fth)
    session.flush()
    p = Provenance(datetime.now(), 'flat.fits', '', 'flatthething')
    p.diskfile_id = df.id
    session.add(p)
    session.flush()
    ph = ProvenanceHistory(datetime.now(), datetime.now(), 'primitive', 'args')
    ph.diskfile_id = df.id
    session.add(ph)
    session.flush()

    report('%s' % df.id)

    assert(mock_context.resp.status == 200)
    assert('This is the full text of the header' in mock_context.resp.stuff)
    assert('flatthething' in mock_context.resp.stuff)
    assert('primitive' in mock_context.resp.stuff)

    session.rollback()