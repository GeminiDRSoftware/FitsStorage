
import pytest

import fits_storage
from fits_storage.web.list_headers import list_headers
from fits_storage.web.observing_statistics import observing_statistics
from tests.orm_helper import MockHeader
from tests.web_helper import MockContext




@pytest.mark.usefixtures("rollback")
def test_observing_statistics(session, monkeypatch):
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

    observing_statistics({'telescope': 'Gemini-North', 'date': '20200101'})
    assert('UTdate' in mock_context.resp.stuff)
