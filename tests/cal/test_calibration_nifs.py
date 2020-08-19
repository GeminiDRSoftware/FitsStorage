
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


def _init_nifs(session):
    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_flat(session):
    _init_nifs(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        test_flat_file = 'N20150505S0297.fits'
        data_file = 'N20150505S0119.fits'

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


@pytest.mark.usefixtures("rollback")
def test_lampoff_flat(session):
    _init_nifs(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        test_lampoff_flat_file = 'N20130708S0453.fits'
        data_file = 'N20130708S0448.fits'

        fits_storage_config.storage_root = '/tmp'

        ensure_file(test_lampoff_flat_file, '/tmp')
        ensure_file(data_file, '/tmp')

        iq = IngestQueueUtil(session, EmptyLogger())

        iq.ingest_file(test_lampoff_flat_file, "", False, True)
        iq.ingest_file(data_file, "", False, True)

        df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
            .filter(DiskFile.canonical == True).one()
        header = session.query(Header).filter(Header.diskfile_id == df.id).one()
        cache_associations(session, header.id)

        df = session.query(DiskFile).filter(DiskFile.filename == test_lampoff_flat_file)\
            .filter(DiskFile.canonical == True).one()
        cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

        cc = session.query(CalCache).filter(CalCache.obs_hid == header.id) \
            .filter(CalCache.cal_hid == cal_header.id).one_or_none()
        assert(cc is not None)
    finally:
        fits_storage_config.storage_root = save_storage_root


@pytest.mark.usefixtures("rollback")
def test_arc(session):
    _init_nifs(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        test_arc_file = 'N20200107S0011.fits'
        data_file = 'N20200119S0172.fits'

        fits_storage_config.storage_root = '/tmp'

        ensure_file(test_arc_file, '/tmp')
        ensure_file(data_file, '/tmp')

        iq = IngestQueueUtil(session, EmptyLogger())

        iq.ingest_file(test_arc_file, "", False, True)
        iq.ingest_file(data_file, "", False, True)

        df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
            .filter(DiskFile.canonical == True).one()
        header = session.query(Header).filter(Header.diskfile_id == df.id).one()
        cache_associations(session, header.id)

        df = session.query(DiskFile).filter(DiskFile.filename == test_arc_file)\
            .filter(DiskFile.canonical == True).one()
        cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

        cc = session.query(CalCache).filter(CalCache.obs_hid == header.id) \
            .filter(CalCache.cal_hid == cal_header.id).one_or_none()
        assert(cc is not None)
    finally:
        fits_storage_config.storage_root = save_storage_root


@pytest.mark.usefixtures("rollback")
def test_ronchi_mask(session):
    _init_nifs(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        test_ronchi_mask_file = 'N20181210S0139.fits'
        data_file = 'N20181210S0095.fits'

        fits_storage_config.storage_root = '/tmp'

        ensure_file(test_ronchi_mask_file, '/tmp')
        ensure_file(data_file, '/tmp')

        iq = IngestQueueUtil(session, EmptyLogger())

        iq.ingest_file(test_ronchi_mask_file, "", False, True)
        iq.ingest_file(data_file, "", False, True)

        df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
            .filter(DiskFile.canonical == True).one()
        header = session.query(Header).filter(Header.diskfile_id == df.id).one()
        cache_associations(session, header.id)

        df = session.query(DiskFile).filter(DiskFile.filename == test_ronchi_mask_file)\
            .filter(DiskFile.canonical == True).one()
        cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

        cc = session.query(CalCache).filter(CalCache.obs_hid == header.id) \
            .filter(CalCache.cal_hid == cal_header.id).one_or_none()
        assert(cc is not None)
    finally:
        fits_storage_config.storage_root = save_storage_root


@pytest.mark.usefixtures("rollback")
def test_telluric_standard(session):
    _init_nifs(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        test_telluric_standard_file = 'N20180906S0058.fits'
        data_file = 'N20180906S0039.fits'

        fits_storage_config.storage_root = '/tmp'

        ensure_file(test_telluric_standard_file, '/tmp')
        ensure_file(data_file, '/tmp')

        iq = IngestQueueUtil(session, EmptyLogger())

        iq.ingest_file(test_telluric_standard_file, "", False, True)
        iq.ingest_file(data_file, "", False, True)

        df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
            .filter(DiskFile.canonical == True).one()
        header = session.query(Header).filter(Header.diskfile_id == df.id).one()
        cache_associations(session, header.id)

        df = session.query(DiskFile).filter(DiskFile.filename == test_telluric_standard_file)\
            .filter(DiskFile.canonical == True).one()
        cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

        cc = session.query(CalCache).filter(CalCache.obs_hid == header.id) \
            .filter(CalCache.cal_hid == cal_header.id).one_or_none()
        assert(cc is not None)
    finally:
        fits_storage_config.storage_root = save_storage_root

