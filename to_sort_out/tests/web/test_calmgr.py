from datetime import date, time

import pytest

import fits_storage
from fits_storage.web import calmgr
from fits_storage.utils.web import Return
from fits_storage.web.calmgr import calmgr, _cal_eval
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


def test_cal_eval():
    from datetime import datetime
    str = '{\'foo\': "foo", \'num\': 1, \'section\': Section(x1=1, x2=2, y1=3, y2=4), ' \
          '\'coeffs\': NonLinCoeffs(1, \'bar\', 3.4, True), \'dt\': datetime.datetime(2020, 1, 1, 0, 0),' \
          '\'bl\': True, \'nn\': None}'
    val = _cal_eval(str)
    assert(len(val.keys()) == 7)
    assert(val["foo"] == "foo")
    assert(val["num"] == 1)
    assert(val["section"] == "[1, 2, 3, 4]")
    assert(val["coeffs"] == '')
    assert(val["dt"] == datetime(2020, 1, 1, 0, 0))
    assert(val["bl"] == True)
    assert(val["nn"] is None)
    # now try datetime at start
    val = _cal_eval('{\'dt\': datetime.datetime(2020, 1, 1, 0, 0), \'foo\': \'foo\'}')
    assert(len(val.keys()) == 2)
    assert(val['foo'] == 'foo')
    assert(val['dt'] == datetime(2020, 1, 1, 0, 0))
    # now try datetime at end
    val = _cal_eval('{\'foo\': \'foo\', \'dt\': datetime.datetime(2020, 1, 1, 0, 0)}')
    assert(len(val.keys()) == 2)
    assert(val['foo'] == 'foo')
    assert(val['dt'] == datetime(2020, 1, 1, 0, 0))
    # now try datetime alone
    val = _cal_eval('{\'dt\': datetime.datetime(2020, 1, 1, 0, 0)}')
    assert(len(val.keys()) == 1)
    assert(val['dt'] == datetime(2020, 1, 1, 0, 0))
    # now try date
    val = _cal_eval('{\'dt\': datetime.date(2020, 1, 1), \'foo\': \'foo\'}')
    assert(len(val.keys()) == 2)
    assert(val['foo'] == 'foo')
    assert(val['dt'] == date(2020, 1, 1))
    # now try time
    val = _cal_eval('{\'dt\': datetime.time(13, 12, 11), \'foo\': \'foo\'}')
    assert(len(val.keys()) == 2)
    assert(val['foo'] == 'foo')
    assert(val['dt'] == time(13, 12, 11))



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
