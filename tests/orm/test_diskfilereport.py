import astrodata
import fits_storage.fits_storage_config as fsc
from gemini_obs_db.diskfile import DiskFile
from fits_storage.orm.diskfilereport import DiskFileReport
from gemini_obs_db.file import File
from tests.file_helper import ensure_file


def test_diskfilereport_noreports():
    save_storage_root = fsc.storage_root
    try:
        fsc.storage_root = '/tmp'
        testfile = 'S20181231S0120.fits'
        ensure_file(testfile, "/tmp")
        f = File(testfile)
        df = DiskFile(f, testfile, "")
        df.ad_object == astrodata.open(df.fullpath())
        dfr = DiskFileReport(df, True, True)
        assert(dfr.mdreport is None)
        assert(dfr.fvreport is None)
        assert(dfr.mdstatus is None)
    finally:
        fsc.storage_root = save_storage_root


def test_diskfilereport_fvreport():
    save_storage_root = fsc.storage_root
    try:
        fsc.storage_root = '/tmp'
        testfile = 'S20181231S0120.fits'
        ensure_file(testfile, "/tmp")
        f = File(testfile)
        df = DiskFile(f, testfile, "")
        dfr = DiskFileReport(df, False, True)
        assert(dfr.mdreport is None)
        assert('fitsverify' in dfr.fvreport)
        assert(dfr.mdstatus is None)
    finally:
        fsc.storage_root = save_storage_root


def test_diskfilereport_mdreport():
    save_storage_root = fsc.storage_root
    try:
        fsc.storage_root = '/tmp'
        testfile = 'S20181231S0120.fits'
        ensure_file(testfile, "/tmp")
        f = File(testfile)
        df = DiskFile(f, testfile, "")
        df.ad_object = astrodata.open(df.fullpath())
        dfr = DiskFileReport(df, True, False)
        assert(dfr.mdreport == 'This looks like a valid file')
        assert(dfr.fvreport is None)
        assert(dfr.mdstatus == 'CORRECT')
    finally:
        fsc.storage_root = save_storage_root
