import datetime

import pytest

import fits_storage
from fits_storage.utils.web import Return
from fits_storage.web.calibrations import interval_hours, interval_string, calibrations
from fits_storage.web.calmgr import calmgr
from tests.web_helper import MockContext


@pytest.mark.usefixtures("rollback")
def test_calmgr_disallow_open_get_req(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context

    monkeypatch.setattr(fits_storage.web.calmgr, "get_context", _mock_get_context)
    calmgr({})
    assert(mock_context.resp.error is not None)
    assert(mock_context.resp.error.code == Return.HTTP_NOT_ACCEPTABLE)
    assert('open query' in mock_context.resp.error.message)

    mock_context = MockContext(session, method='POST')
    calmgr({})
    assert(mock_context.resp.error is not None)
    assert(mock_context.resp.error.code == Return.HTTP_METHOD_NOT_ALLOWED)
    assert('No calibration type' in mock_context.resp.error.message)

    mock_context = MockContext(session, method='GET')
    r = calmgr({'date': '20200101'})
    assert(mock_context.resp.error is None)
    assert(r is not None)
    assert(r['req_method'] == 'GET')
