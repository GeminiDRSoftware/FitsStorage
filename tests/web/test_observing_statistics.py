from datetime import datetime, timedelta

import pytest

import fits_storage
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.file import File
from fits_storage.orm.fulltextheader import FullTextHeader
from fits_storage.orm.header import Header
from fits_storage.web.list_headers import list_headers
from fits_storage.web.observing_statistics import observing_statistics
from tests.file_helper import setup_mock_file_stuff
from tests.orm_helper import MockHeader
from tests.web_helper import MockContext




@pytest.mark.usefixtures("rollback")
def test_observing_statistics(session, monkeypatch):
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
    h.data_label = 'datalabel'
    h.instrument = 'ALOPEKE'
    session.add(h)
    session.flush()
    fth = FullTextHeader(df)
    fth.fulltext = 'This is the full text of the header'
    session.add(fth)
    session.flush()

    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context

    def _mock_list_headers(selection, orderby):
        h = MockHeader()
        retval = list()
        retval.append(h)
        return retval

    def _mock_nightlog_numbers(utdate, telescope):
        dict = {}
        dict['T_night'] = 0
        dict['T_science'] = 0
        dict['T_eng'] = 0
        dict['T_fr'] = 0
        dict['T_weather'] = 0
        dict['error_message'] = 'test'
        return dict

    monkeypatch.setattr(fits_storage.web.observing_statistics, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.list_headers, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.utils.web.adapter, "get_context", _mock_get_context)

    monkeypatch.setattr(fits_storage.web.observing_statistics, "list_headers", _mock_list_headers)
    # stub nightlog_numbers so it doesn't query the webservice
    monkeypatch.setattr(fits_storage.web.observing_statistics, "nightlog_numbers", _mock_nightlog_numbers)

    fromdt = h.ut_datetime - timedelta(days=2)
    todt = h.ut_datetime + timedelta(days=2)
    observing_statistics({'telescope': 'Gemini-North', 'date': h.ut_datetime.strftime('%Y%m%d')})
    assert(mock_context.resp.status == 200)
    assert('UTdate' in mock_context.resp.stuff)
    mock_context.resp.stuff = ''
    observing_statistics({'telescope': 'Gemini-North',
                          'daterange': '%s-%s' % (fromdt.strftime('%Y%m%d'), todt.strftime('%Y%m%d'))})
    assert(mock_context.resp.status == 200)
    assert('UTdate' in mock_context.resp.stuff)
    mock_context.resp.stuff = ''
    observing_statistics({'telescope': 'Gemini-North'})
    assert(mock_context.resp.status == 200)
    assert('UTdate' in mock_context.resp.stuff)
