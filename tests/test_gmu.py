import os

import pytz
import time

import pytest
import itertools

import gemini_obs_db.utils.gemini_metadata_utils as gmu
import datetime
from dateutil import parser as dateparser
parsedate = dateparser.parse

gemini_telescope_pairs = (
    ('gEmInI-NoRtH', 'Gemini-North'),
    ('GeMiNi-sOuTh', 'Gemini-South'),
    ('Gemini North', None),
    ('foobar', None)
    )


@pytest.mark.parametrize("input,expected", gemini_telescope_pairs)
def test_gemini_telescope(input, expected):
    assert gmu.gemini_telescope(input) == expected


gemini_instrument_pairs = (
    ('nIRi',            'NIRI'),
    ('NiFs',            'NIFS'),
    ('GMos-N',          'GMOS-N'),
    ('gmOS-s',          'GMOS-S'),
    ('MIcHElLE',        'michelle'),
    ('gnIrs',           'GNIRS'),
    ('phOenIx',         'PHOENIX'),
    ('texes',           'TEXES'),
    ('trEcs',           'TReCS'),
    ('NicI',            'NICI'),
    ('gSAOi',           'GSAOI'),
    ('OsciR',           'OSCIR'),
    ('f2',              'F2'),
    ('gPI',             'GPI'),
    ('ABU',             'ABU'),
    ('bHRos',           'bHROS'),
    ('hrwFS',           'hrwfs'),
    ('fLAmingos',       'FLAMINGOS'),
    ('cirPAss',         'CIRPASS'),
    ('Hokupa+QUIRC',    'Hokupaa+QUIRC'),
    ('Hokupaa+QUIRC',   'Hokupaa+QUIRC'),
    ('Hokupa++QUIRC',   'Hokupaa+QUIRC'),
    ('Hokupaa++QUIRC',  'Hokupaa+QUIRC'),
    ('gmos',            (dict(gmos=True), 'GMOS')),
    ('blah',            (dict(other=True), 'blah')),
    )


@pytest.mark.parametrize("input,expected", gemini_instrument_pairs)
def test_gemini_instrument(input, expected):
    if isinstance(expected, tuple):
        kwargs, expected = expected
        assert gmu.gemini_instrument(input, **kwargs) == expected
    else:
        assert gmu.gemini_instrument(input) == expected


def generate_same_pairs_plus_garbage(data, garbage, bad_value = None):
    for k in data:
        yield (k, k)

    for k in garbage:
        yield (k, bad_value)


gemini_observation_type_bad  = ('dark', 'blah')
gemini_observation_class_bad = ('DAYCAL', 'blah')
gemini_reduction_state_bad   = ('raw', 'blah')
gemini_cal_type_bad          = ('DARK', 'blah')
gemini_gmos_grating_bad      = ('mirror', 'blah')


def good_binning():
    for a, b in itertools.product("124", "124"):
        yield a + 'x' + b


gemini_binning_bad = ('1x6', '1x', 'blah')
good_dateranges = (
    '20050101-20061231',
    '20080228-20120229',
    '19900101-20390101',
    )
bad_dateranges = (
    '10000101-20000101',
    '19990101-29000101',
    'blah',
    ''
    )
daterange_as_datetime_pairs = (
    ('20050101-20061231', (parsedate('20050101'), parsedate('20061231'))),
    ('20080228-20120229', (parsedate('20080228'), parsedate('20120229'))),
    ('blah', None)
    )

good_dates = ('20050101', '20080228', '20120229', '20390101')
bad_dates = ('00000000', '20110229', '12345678', '10000101', '29000101', 'blah', '')


def date_as_datetime_pairs():
    for d in good_dates:
        yield (d, parsedate(d))
    for d in bad_dates:
        yield (d, None)
    # for f, d in more_good_dates:
    #     yield (f, parsedate(d))


@pytest.mark.parametrize("input,expected",
        generate_same_pairs_plus_garbage(gmu.obs_types,
                                         gemini_observation_type_bad))
def test_gemini_observation_type(input, expected):
    assert gmu.gemini_observation_type(input) == expected


@pytest.mark.parametrize("input,expected",
        generate_same_pairs_plus_garbage(gmu.obs_classes,
                                         gemini_observation_class_bad))
def test_gemini_observation_class(input, expected):
    assert gmu.gemini_observation_class(input) == expected


@pytest.mark.parametrize("input,expected",
        generate_same_pairs_plus_garbage(gmu.reduction_states,
                                         gemini_reduction_state_bad))
def test_gemini_reduction_state(input, expected):
    assert gmu.gemini_reduction_state(input) == expected


@pytest.mark.parametrize("input,expected",
        generate_same_pairs_plus_garbage(gmu.cal_types,
                                         gemini_cal_type_bad))
def test_gemini_caltype(input, expected):
    assert gmu.gemini_caltype(input) == expected


@pytest.mark.parametrize("input,expected",
        generate_same_pairs_plus_garbage(gmu.gmos_gratings,
                                         gemini_gmos_grating_bad, ''))
def test_gmos_gratingname(input, expected):
    assert gmu.gmos_gratingname(input) == expected


@pytest.mark.parametrize("input,expected",
        generate_same_pairs_plus_garbage(good_binning(), gemini_binning_bad, ''))
def test_gemini_binning(input, expected):
    assert gmu.gemini_binning(input) == expected


@pytest.mark.parametrize("input,expected",
        generate_same_pairs_plus_garbage(good_dates, bad_dates, ''))
def test_gemini_date(input, expected):
    assert gmu.gemini_date(input) == expected


@pytest.mark.parametrize("input,expected", date_as_datetime_pairs())
def test_gemini_date_as_datetime(input, expected):
    assert gmu.gemini_date(input, as_datetime=True) == expected


@pytest.mark.parametrize("input,expected",
        generate_same_pairs_plus_garbage(good_dateranges, bad_dateranges, ''))
def test_gemini_daterange(input, expected):
    assert gmu.gemini_daterange(input) == expected


@pytest.mark.parametrize("input,expected", daterange_as_datetime_pairs)
def test_gemini_daterange_as_datetime(input, expected):
    assert gmu.gemini_daterange(input, as_datetime=True) == expected


# These are for the below test with updated today/etc parsing for Chile and Hawaii
CHILE_TZ = pytz.timezone('America/Santiago')  # timezone('America/Santiago')
HAWAII_TZ = pytz.timezone('US/Hawaii')
FAKE_TIME = datetime.datetime(2020, 5, 11, 14, 30, 55)
CHILE_NOON = CHILE_TZ.localize(datetime.datetime(2020, 5, 11, 12, 00, 00))
CHILE_AFTERNOON = CHILE_TZ.localize(datetime.datetime(2020, 5, 11, 16, 00, 00))
CHILE_EVENING = CHILE_TZ.localize(datetime.datetime(2020, 5, 11, 21, 00, 00))
HAWAII_NOON = HAWAII_TZ.localize(datetime.datetime(2020, 5, 11, 12, 00, 00))
HAWAII_AFTERNOON = HAWAII_TZ.localize(datetime.datetime(2020, 5, 11, 16, 00, 00))
HAWAII_EVENING = HAWAII_TZ.localize(datetime.datetime(2020, 5, 11, 21, 00, 00))
HAWAII_EARLY_MORNING = HAWAII_TZ.localize(datetime.datetime(2020, 5, 11, 2, 00, 00))


@pytest.fixture
def patch_datetime_now(monkeypatch):
    class mydatetime:
        @classmethod
        def utcnow(cls):
            return FAKE_TIME.astimezone(tz=datetime.timezone.utc).replace(tzinfo=None)

        @classmethod
        def now(cls):
            return FAKE_TIME

    monkeypatch.setattr(datetime, 'datetime', mydatetime)

#
# def test_today_tommorow_yesterday(patch_datetime_now):
#     os.environ['TZ'] = 'America/Santiago'
#     time.tzset()
#
#     global FAKE_TIME
#     FAKE_TIME = CHILE_NOON
#     # Today, noon Chile (so 'yesterday' 2pm Chile through 2pm today)
#     startdt, enddt = gmu.get_time_period('today')
#     assert startdt.year == 2020
#     assert startdt.month == 5
#     assert startdt.day == 11
#     assert startdt.hour == 14
#     assert startdt.minute == 0
#     assert startdt.second == 0
#     assert enddt.year == 2020
#     assert enddt.month == 5
#     assert enddt.day == 12
#     assert enddt.hour == 14
#     assert enddt.minute == 0
#     assert enddt.second == 0
#
#     # Yesterday, noon Chile (so 'day before yesterday' 2pm Chile through 2pm yesterday)
#     FAKE_TIME = CHILE_NOON
#     startdt, enddt = gmu.get_time_period('yesterday')
#     assert startdt.year == 2020
#     assert startdt.month == 5
#     assert startdt.day == 10
#     assert startdt.hour == 14
#     assert startdt.minute == 0
#     assert startdt.second == 0
#     assert enddt.year == 2020
#     assert enddt.month == 5
#     assert enddt.day == 11
#     assert enddt.hour == 14
#     assert enddt.minute == 0
#     assert enddt.second == 0
#
#     # Today, afternoon Chile (so uses today into tonight)
#     FAKE_TIME = CHILE_AFTERNOON
#     startdt, enddt = gmu.get_time_period('today')
#     assert startdt.year == 2020
#     assert startdt.month == 5
#     assert startdt.day == 12
#     assert startdt.hour == 14
#     assert startdt.minute == 0
#     assert startdt.second == 0
#     assert enddt.year == 2020
#     assert enddt.month == 5
#     assert enddt.day == 13
#     assert enddt.hour == 14
#     assert enddt.minute == 0
#     assert enddt.second == 0
#
#     # Today, evening Chile (so UTC rolled)
#     FAKE_TIME = CHILE_EVENING
#     startdt, enddt = gmu.get_time_period('today')
#     assert startdt.year == 2020
#     assert startdt.month == 5
#     assert startdt.day == 12
#     assert startdt.hour == 14
#     assert startdt.minute == 0
#     assert startdt.second == 0
#     assert enddt.year == 2020
#     assert enddt.month == 5
#     assert enddt.day == 13
#     assert enddt.hour == 14
#     assert enddt.minute == 0
#     assert enddt.second == 0
#
#     os.environ['TZ'] = 'US/Hawaii'
#     time.tzset()
#     FAKE_TIME = HAWAII_NOON
#     startdt, enddt = gmu.get_time_period('today')
#     assert startdt.year == 2020
#     assert startdt.month == 5
#     assert startdt.day == 11
#     assert startdt.hour == 14
#     assert startdt.minute == 0
#     assert startdt.second == 0
#     assert enddt.year == 2020
#     assert enddt.month == 5
#     assert enddt.day == 12
#     assert enddt.hour == 14
#     assert enddt.minute == 0
#     assert enddt.second == 0
#
#     FAKE_TIME = HAWAII_AFTERNOON
#     startdt, enddt = gmu.get_time_period('today')
#     assert startdt.year == 2020
#     assert startdt.month == 5
#     assert startdt.day == 12
#     assert startdt.hour == 14
#     assert startdt.minute == 0
#     assert startdt.second == 0
#     assert enddt.year == 2020
#     assert enddt.month == 5
#     assert enddt.day == 13
#     assert enddt.hour == 14
#     assert enddt.minute == 0
#     assert enddt.second == 0
#
#     FAKE_TIME = HAWAII_EVENING
#     startdt, enddt = gmu.get_time_period('today')
#     assert startdt.year == 2020
#     assert startdt.month == 5
#     assert startdt.day == 12
#     assert startdt.hour == 14
#     assert startdt.minute == 0
#     assert startdt.second == 0
#     assert enddt.year == 2020
#     assert enddt.month == 5
#     assert enddt.day == 13
#     assert enddt.hour == 14
#     assert enddt.minute == 0
#     assert enddt.second == 0
#
#     FAKE_TIME = HAWAII_EARLY_MORNING
#     startdt, enddt = gmu.get_time_period('today')
#     assert startdt.year == 2020
#     assert startdt.month == 5
#     assert startdt.day == 11
#     assert startdt.hour == 14
#     assert startdt.minute == 0
#     assert startdt.second == 0
#     assert enddt.year == 2020
#     assert enddt.month == 5
#     assert enddt.day == 12
#     assert enddt.hour == 14
#     assert enddt.minute == 0
#     assert enddt.second == 0


def test_date_string(patch_datetime_now):
    os.environ['TZ'] = 'America/Santiago'
    time.tzset()

    startdt, enddt = gmu.get_time_period('20200511')
    assert startdt.year == 2020
    assert startdt.month == 5
    assert startdt.day == 10
    assert startdt.hour == 17
    assert startdt.minute == 0
    assert startdt.second == 0
    assert enddt.year == 2020
    assert enddt.month == 5
    assert enddt.day == 11
    assert enddt.hour == 17
    assert enddt.minute == 0
    assert enddt.second == 0

    os.environ['TZ'] = 'US/Hawaii'
    time.tzset()

    startdt, enddt = gmu.get_time_period('20200511')
    assert startdt.year == 2020
    assert startdt.month == 5
    assert startdt.day == 11
    assert startdt.hour == 0
    assert startdt.minute == 0
    assert startdt.second == 0
    assert enddt.year == 2020
    assert enddt.month == 5
    assert enddt.day == 12
    assert enddt.hour == 0
    assert enddt.minute == 0
    assert enddt.second == 0
