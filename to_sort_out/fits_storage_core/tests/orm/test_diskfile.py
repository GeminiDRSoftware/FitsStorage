from datetime import datetime

from gemini_obs_db.orm.diskfile import _determine_timestamp_from_filename, DiskFile
from gemini_obs_db.orm.file import File
from tests.file_helper import ensure_file
import gemini_obs_db.db_config as dbc


def test_standard_filename():
    dt = _determine_timestamp_from_filename('N20200501S0101.fits')
    assert(dt == datetime(year=2020, month=5, day=1))
    dt = _determine_timestamp_from_filename('N20200501S0101-stuff.fits')
    assert(dt == datetime(year=2020, month=5, day=1))


def test_igrins_filename():
    dt = _determine_timestamp_from_filename('SDCK_20200210_0071.fits')
    assert(dt == datetime(year=2020, month=2, day=10))


def test_skycam_filename():
    dt = _determine_timestamp_from_filename('img_20170810_11h03m35s.fits')
    assert(dt == datetime(year=2017, month=8, day=10))


def test_bizzare_filename():
    dt = _determine_timestamp_from_filename('dSgeEDDs323---20200102-4-334.fits')
    assert(dt == datetime(year=2020, month=1, day=2))


def test_bad_filename():
    dt = _determine_timestamp_from_filename('asdf-271.fits')
    assert(dt is None)


def test_diskfile():
    save_storage_root = dbc.storage_root
    try:
        dbc.storage_root = '/tmp'
        testfile = 'S20181231S0120.fits'
        ensure_file(testfile, "/tmp")
        f = File(testfile)
        df = DiskFile(f, testfile, "")
        assert(df.filename == testfile)
        assert(df.path == "")
        assert(df.present is True)
        assert(df.canonical is True)
        assert(df.entrytime is not None)
        assert(df.get_file_size() == 16801920)
        assert(df.get_file_md5() == '1402251d036b931fbfbf0bb2db0d2c38')
        assert(df.file_size == 16801920)
        assert(df.file_md5 == '1402251d036b931fbfbf0bb2db0d2c38')
        assert(df.get_data_size() == 16801920)
        assert(df.get_data_md5() == '1402251d036b931fbfbf0bb2db0d2c38')
        assert(df.data_size == 16801920)
        assert(df.data_md5 == '1402251d036b931fbfbf0bb2db0d2c38')
        assert(df.compressed is False)
    finally:
        dbc.storage_root = save_storage_root


def test_diskfile_fullpath():
    save_storage_root = dbc.storage_root
    try:
        dbc.storage_root = '/tmp/jenkins_pytest/dataflow'
        testfile = 'S20181231S0120.fits'
        ensure_file(testfile, "/tmp/jenkins_pytest/dataflow")
        f = File(testfile)
        df = DiskFile(f, testfile, "")
        assert(df.fullpath() == '/tmp/jenkins_pytest/dataflow/%s' % testfile)
    finally:
        dbc.storage_root = save_storage_root
