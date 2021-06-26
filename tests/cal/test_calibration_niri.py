
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


def _init_niri(session):
    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_dark(session):
    _init_niri(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        test_dark_file = 'N20180329S0376.fits'
        data_file = 'N20180329S0134.fits'

        fits_storage_config.storage_root = '/tmp'

        ensure_file(test_dark_file, '/tmp')
        ensure_file(data_file, '/tmp')

        iq = IngestQueueUtil(session, EmptyLogger())

        iq.ingest_file(test_dark_file, "", False, True)
        iq.ingest_file(data_file, "", False, True)

        df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
            .filter(DiskFile.canonical == True).one()
        header = session.query(Header).filter(Header.diskfile_id == df.id).one()
        cache_associations(session, header.id)

        df = session.query(DiskFile).filter(DiskFile.filename == test_dark_file)\
            .filter(DiskFile.canonical == True).one()
        cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

        cc = session.query(CalCache).filter(CalCache.obs_hid == header.id) \
            .filter(CalCache.cal_hid == cal_header.id).one_or_none()
        assert(cc is not None)
    finally:
        fits_storage_config.storage_root = save_storage_root


@pytest.mark.usefixtures("rollback")
def test_flat(session):
    _init_niri(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        test_flat_file = 'N20180318S0207.fits'
        data_file = 'N20180318S0044.fits'

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
def test_arc(session):
    _init_niri(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        test_arc_file = 'N20100101S0436.fits'
        data_file = 'N20100101S0402.fits'

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
def test_lampoff_flat(session):
    _init_niri(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        test_lampoff_flat_file = 'N20130620S0531.fits'
        data_file = 'N20130620S0543.fits'

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
def test_telluric_standard(session):
    _init_niri(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        test_telluric_standard_file = 'N20120505S0159.fits'
        data_file = 'N20120505S0147.fits'

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

