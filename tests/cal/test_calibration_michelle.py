
import pytest

from fits_storage.cal import get_cal_object
from gemini_obs_db.calcache import CalCache
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.header import Header
from fits_storage.utils.calcachequeue import CalCacheQueueUtil, cache_associations
from fits_storage.utils.ingestqueue import IngestQueueUtil
from fits_storage.utils.null_logger import EmptyLogger

from fits_storage import fits_storage_config
from tests.file_helper import ensure_file


def _mock_sendmail(fromaddr, toaddr, message):
    pass


def _init_michelle(session):
    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_flat(session):
    _init_michelle(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        test_flat_file = 'N20100119S0203.fits'
        data_file = 'N20100119S0080.fits'

        fits_storage_config.storage_root = '/tmp'

        ensure_file(test_flat_file, '/tmp')
        ensure_file(data_file, '/tmp')

        iq = IngestQueueUtil(session, EmptyLogger())

        iq.ingest_file(test_flat_file, "", False, True)
        iq.ingest_file(data_file, "", False, True)

        df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
            .filter(DiskFile.canonical == True).one()
        header = session.query(Header).filter(Header.diskfile_id == df.id).one()
        cache_associations(session, header.id)

        df = session.query(DiskFile).filter(DiskFile.filename == test_flat_file)\
            .filter(DiskFile.canonical == True).one()
        cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

        cc = session.query(CalCache).filter(CalCache.obs_hid == header.id) \
            .filter(CalCache.cal_hid == cal_header.id).one_or_none()
        assert(cc is not None)
    finally:
        fits_storage_config.storage_root = save_storage_root
