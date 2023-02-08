
import pytest

import fits_storage
from fits_storage.web.list_headers import list_headers, list_obslogs
from fits_storage.web.logcomments import log_comments
from tests.web_helper import MockContext


@pytest.mark.usefixtures("rollback")
def test_logcomments(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context

    monkeypatch.setattr(fits_storage.web.logcomments, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)
    log_comments({'date': '20200101'})
    assert('DOCTYPE' in mock_context.resp.stuff)
