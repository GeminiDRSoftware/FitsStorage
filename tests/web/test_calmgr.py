import datetime

import pytest

import fits_storage
from fits_storage.web import calmgr
from fits_storage.cal import CalibrationGMOS
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.file import File
from gemini_obs_db.header import Header
from fits_storage.utils.web import Return
from fits_storage.web.calibrations import interval_hours, interval_string, calibrations
from fits_storage.web.calmgr import calmgr, cals_info
from tests.file_helper import setup_mock_file_stuff
from tests.web_helper import MockContext


# TODO never calls cals_info - WTF?!
# @pytest.mark.usefixtures("rollback")
# def test_calmgr_cals_info(session, monkeypatch):
#     setup_mock_file_stuff(monkeypatch)
#
#     f = File('bar.fits')
#     session.add(f)
#     session.flush()
#     df = DiskFile(f, 'bar.fits', '')
#     df.canonical = True
#     session.add(df)
#     session.flush()
#     h = Header(df)
#     h.ut_datetime = datetime.datetime.now()
#     session.add(h)
#     session.flush()
#
#     mock_context = MockContext(session, method='GET')
#
#     def _mock_get_context(initialize=True):
#         return mock_context
#
#     monkeypatch.setattr(fits_storage.web.calmgr, "get_context", _mock_get_context)
#     monkeypatch.setattr(fits_storage.utils.web.adapter, "get_context", _mock_get_context)
#     monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)
#
#     cal_obj = CalibrationGMOS
#     cals_info(None, 'flat', hostname='hostname')
#     assert(mock_context.resp.status == 200)
#
#     session.rollback()


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
