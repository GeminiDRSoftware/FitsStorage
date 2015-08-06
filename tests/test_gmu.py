import pytest
import itertools
import fits_storage.gemini_metadata_utils as gmu
from datetime import datetime, timedelta
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
gemini_binning_bad           = ('1x6', '1x', 'blah')
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
now = datetime.utcnow()
a_day_ago = now - timedelta(days = 1)
more_good_dates = (
    ('today'  ,   '{0:%Y%m%d}'.format(now)),
    ('tonight',   '{0:%Y%m%d}'.format(now)),
    ('yesterday', '{0:%Y%m%d}'.format(a_day_ago)),
    ('lastnight', '{0:%Y%m%d}'.format(a_day_ago)),
    )

def date_as_datetime_pairs():
    for d in good_dates:
        yield (d, parsedate(d))
    for d in bad_dates:
        yield (d, None)
    for f, d in more_good_dates:
        yield (f, parsedate(d))

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
        itertools.chain(generate_same_pairs_plus_garbage(good_dates, bad_dates, ''),
                        more_good_dates))
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
