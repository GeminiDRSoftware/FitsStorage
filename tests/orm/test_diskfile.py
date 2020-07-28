from datetime import datetime

from fits_storage.orm.diskfile import _determine_timestamp_from_filename


def test_standard_filename():
    dt = _determine_timestamp_from_filename('N20200501S0101.fits')
    assert(dt == datetime(year=2020, month=5, day=1))
    dt = _determine_timestamp_from_filename('N20200501S0101-stuff.fits')
    assert(dt == datetime(year=2020, month=5, day=1))


def test_future_filename():
    dt = _determine_timestamp_from_filename('N20300501S0101.fits')
    assert(dt is None)


def test_igrins_filename():
    dt = _determine_timestamp_from_filename('SDCK_20200210_0071.fits')
    assert(dt == datetime(year=2020, month=2, day=10))


def test_skycam_filename():
    dt = _determine_timestamp_from_filename('img_20170810_11h03m35s.fits')
    assert(dt == datetime(year=2018, month=8, day=10))


def test_bizzare_filename():
    dt = _determine_timestamp_from_filename('dSgeEDDs323---20200102-4-334.fits')
    assert(dt == datetime(year=2020, month=1, day=2))


def test_bad_filename():
    dt = _determine_timestamp_from_filename('asdf-271.fits')
    assert(dt is None)
