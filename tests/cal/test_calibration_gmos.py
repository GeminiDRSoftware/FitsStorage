
import pytest

from fits_storage.cal import get_cal_object
from fits_storage.orm.calcache import CalCache
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.header import Header
from fits_storage.utils.calcachequeue import CalCacheQueueUtil, cache_associations
from fits_storage.utils.ingestqueue import IngestQueueUtil
from fits_storage.utils.null_logger import EmptyLogger

from fits_storage import fits_storage_config


def _mock_sendmail(fromaddr, toaddr, message):
    pass


def _init_gmos(session):
    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_standard(session):
    _init_gmos(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        # TODO work in progress
        # TODO migrate to some sort of config singleton that we can easily customize for pytests
        fits_storage_config.storage_root='testdata/test_calibration_gmos'
        print("storage root: %s" % fits_storage_config.storage_root)
        iq = IngestQueueUtil(session, EmptyLogger())
        iq.ingest_file("N20191212S0083_distortionCorrected.fits", "", False, False)
        iq.ingest_file("N20191103S0033_standard.fits", "", False, False)

        df = session.query(DiskFile).filter(DiskFile.filename == 'N20191212S0083_distortionCorrected.fits')\
            .filter(DiskFile.canonical == True).one()
        header = session.query(Header).filter(Header.diskfile_id == df.id).one()
        cache_associations(session, header.id)

        df = session.query(DiskFile).filter(DiskFile.filename == 'N20191103S0033_standard.fits')\
            .filter(DiskFile.canonical == True).one()
        cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

        cc = session.query(CalCache).filter(CalCache.obs_hid == header.id) \
            .filter(CalCache.cal_hid == cal_header.id).one_or_none()
        assert(cc is not None)
    finally:
        fits_storage_config.storage_root = save_storage_root
