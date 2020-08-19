
import pytest

from fits_storage.cal import get_cal_object
from fits_storage.orm.calcache import CalCache
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.header import Header
from fits_storage.utils.calcachequeue import CalCacheQueueUtil, cache_associations
from fits_storage.utils.ingestqueue import IngestQueueUtil
from fits_storage.utils.null_logger import EmptyLogger

from fits_storage import fits_storage_config
from tests.file_helper import ensure_file


def _mock_sendmail(fromaddr, toaddr, message):
    pass


def _init_gsaoi(session):
    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_domeflat(session):
    _init_gsaoi(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        test_domeflat_file = 'S20181016S0090.fits'
        data_file = 'S20181018S0151.fits'

        fits_storage_config.storage_root = '/tmp'

        ensure_file(test_domeflat_file, '/tmp')
        ensure_file(data_file, '/tmp')

        iq = IngestQueueUtil(session, EmptyLogger())

        iq.ingest_file(test_domeflat_file, "", False, True)
        iq.ingest_file(data_file, "", False, True)

        df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
            .filter(DiskFile.canonical == True).one()
        header = session.query(Header).filter(Header.diskfile_id == df.id).one()
        cache_associations(session, header.id)

        df = session.query(DiskFile).filter(DiskFile.filename == test_domeflat_file)\
            .filter(DiskFile.canonical == True).one()
        cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

        cc = session.query(CalCache).filter(CalCache.obs_hid == header.id) \
            .filter(CalCache.cal_hid == cal_header.id).one_or_none()
        assert(cc is not None)
    finally:
        fits_storage_config.storage_root = save_storage_root


@pytest.mark.usefixtures("rollback")
def test_lampoff_domeflat(session):
    _init_gsaoi(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        test_lampoff_domeflat_file = 'S20181016S0144.fits'
        data_file = 'S20181018S0151.fits'

        fits_storage_config.storage_root = '/tmp'

        ensure_file(test_lampoff_domeflat_file, '/tmp')
        ensure_file(data_file, '/tmp')

        iq = IngestQueueUtil(session, EmptyLogger())

        iq.ingest_file(test_lampoff_domeflat_file, "", False, True)
        iq.ingest_file(data_file, "", False, True)

        df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
            .filter(DiskFile.canonical == True).one()
        header = session.query(Header).filter(Header.diskfile_id == df.id).one()
        cache_associations(session, header.id)

        df = session.query(DiskFile).filter(DiskFile.filename == test_lampoff_domeflat_file)\
            .filter(DiskFile.canonical == True).one()
        cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

        cc = session.query(CalCache).filter(CalCache.obs_hid == header.id) \
            .filter(CalCache.cal_hid == cal_header.id).one_or_none()
        assert(cc is not None)
    finally:
        fits_storage_config.storage_root = save_storage_root


@pytest.mark.usefixtures("rollback")
def test_photometric_standard(session):
    _init_gsaoi(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        test_lampoff_domeflat_file = 'S20181018S0115.fits'
        data_file = 'S20181016S0163.fits'

        fits_storage_config.storage_root = '/tmp'

        ensure_file(test_lampoff_domeflat_file, '/tmp')
        ensure_file(data_file, '/tmp')

        iq = IngestQueueUtil(session, EmptyLogger())

        iq.ingest_file(test_lampoff_domeflat_file, "", False, True)
        iq.ingest_file(data_file, "", False, True)

        df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
            .filter(DiskFile.canonical == True).one()
        header = session.query(Header).filter(Header.diskfile_id == df.id).one()
        cache_associations(session, header.id)

        df = session.query(DiskFile).filter(DiskFile.filename == test_lampoff_domeflat_file)\
            .filter(DiskFile.canonical == True).one()
        cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

        cc = session.query(CalCache).filter(CalCache.obs_hid == header.id) \
            .filter(CalCache.cal_hid == cal_header.id).one_or_none()
        assert(cc is not None)
    finally:
        fits_storage_config.storage_root = save_storage_root
