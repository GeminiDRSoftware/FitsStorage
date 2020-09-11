import datetime

import pytest

import fits_storage
from fits_storage.web.calibrations import interval_hours, interval_string, calibrations
from tests.web_helper import MockContext


class MockHeader(object):
    def __init__(self, ut_datetime):
        self.ut_datetime = ut_datetime


def test_interval_hours():
    a = MockHeader(datetime.datetime(2020,1,1,14,0,0))
    b = MockHeader(datetime.datetime(2020,1,2,19,0,0))

    interval = interval_hours(a, b)
    assert(interval == -29)

    interval = interval_hours(b, a)
    assert(interval == 29)


def test_interval_string():
    a = MockHeader(datetime.datetime(2020,1,1,14,0,0))
    b = MockHeader(datetime.datetime(2020,1,2,19,0,0))
    interval = interval_string(a, b)
    assert(interval == '29.0 hours before')

    interval = interval_string(b, a)
    assert(interval == '29.0 hours after')

    c = MockHeader(datetime.datetime(2020,1,3,11,0,0))
    interval = interval_string(a, c)
    assert(interval == '45.0 hours before')

    c = MockHeader(datetime.datetime(2020,1,5,11,0,0))
    interval = interval_string(a, c)
    assert(interval == '3.9 days before')

    c = MockHeader(datetime.datetime(2020,1,1,14,21,2))
    interval = interval_string(a, c)
    assert(interval == '21.0 minutes before')


@pytest.mark.usefixtures("rollback")
def test_calibrations(session, monkeypatch):
    mock_context = MockContext(session)

    def _mock_get_context(initialize=True):
        return mock_context

    monkeypatch.setattr(fits_storage.web.calibrations, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)
    calibrations({'date': '20200101'})
    assert('DOCTYPE' in mock_context.resp.stuff)
