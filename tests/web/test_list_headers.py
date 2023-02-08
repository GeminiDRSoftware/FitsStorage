
import pytest

import fits_storage
from fits_storage.web.list_headers import list_headers, list_obslogs
from tests.web_helper import MockContext


@pytest.mark.usefixtures("rollback")
def test_list_headers(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context

    monkeypatch.setattr(fits_storage.web.list_headers, "get_context", _mock_get_context)
    r = list_headers({'date': '20200101'}, orderby='ut_timestamp')
    assert(r is not None)


@pytest.mark.usefixtures("rollback")
def test_list_obslogs(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context

    monkeypatch.setattr(fits_storage.web.list_headers, "get_context", _mock_get_context)
    r = list_obslogs({'date': '20200101'}, orderby='ut_timestamp')
    assert(r is not None)
