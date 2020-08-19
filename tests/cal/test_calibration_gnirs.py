
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


def _init_gnirs(session):
    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_dark(session):
    _init_gnirs(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        raw_dark_file = 'N20180524S0080.fits'
        data_file = 'N20180524S0117.fits'

        fits_storage_config.storage_root = '/tmp'

        ensure_file(raw_dark_file, '/tmp')
        ensure_file(data_file, '/tmp')

        iq = IngestQueueUtil(session, EmptyLogger())

        iq.ingest_file(raw_dark_file, "", False, True)
        iq.ingest_file(data_file, "", False, True)

        df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
            .filter(DiskFile.canonical == True).one()
        header = session.query(Header).filter(Header.diskfile_id == df.id).one()
        cache_associations(session, header.id)

        df = session.query(DiskFile).filter(DiskFile.filename == raw_dark_file)\
            .filter(DiskFile.canonical == True).one()
        cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

        cc = session.query(CalCache).filter(CalCache.obs_hid == header.id) \
            .filter(CalCache.cal_hid == cal_header.id).one_or_none()
        assert(cc is not None)
    finally:
        fits_storage_config.storage_root = save_storage_root


@pytest.mark.usefixtures("rollback")
def test_flat(session):
    _init_gnirs(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        raw_flat_file = 'N20180101S0157.fits'
        data_file = 'N20180101S0122.fits'

        fits_storage_config.storage_root = '/tmp'

        ensure_file(raw_flat_file, '/tmp')
        ensure_file(data_file, '/tmp')

        iq = IngestQueueUtil(session, EmptyLogger())

        iq.ingest_file(raw_flat_file, "", False, True)
        iq.ingest_file(data_file, "", False, True)

        df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
            .filter(DiskFile.canonical == True).one()
        header = session.query(Header).filter(Header.diskfile_id == df.id).one()
        cache_associations(session, header.id)

        df = session.query(DiskFile).filter(DiskFile.filename == raw_flat_file)\
            .filter(DiskFile.canonical == True).one()
        cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

        cc = session.query(CalCache).filter(CalCache.obs_hid == header.id) \
            .filter(CalCache.cal_hid == cal_header.id).one_or_none()
        assert(cc is not None)
    finally:
        fits_storage_config.storage_root = save_storage_root


@pytest.mark.usefixtures("rollback")
def test_arc(session):
    _init_gnirs(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        raw_arc_file = 'N20181231S0809.fits'
        data_file = 'N20181231S0579.fits'

        fits_storage_config.storage_root = '/tmp'

        ensure_file(raw_arc_file, '/tmp')
        ensure_file(data_file, '/tmp')

        iq = IngestQueueUtil(session, EmptyLogger())

        iq.ingest_file(raw_arc_file, "", False, True)
        iq.ingest_file(data_file, "", False, True)

        df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
            .filter(DiskFile.canonical == True).one()
        header = session.query(Header).filter(Header.diskfile_id == df.id).one()
        cache_associations(session, header.id)

        df = session.query(DiskFile).filter(DiskFile.filename == raw_arc_file)\
            .filter(DiskFile.canonical == True).one()
        cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

        cc = session.query(CalCache).filter(CalCache.obs_hid == header.id) \
            .filter(CalCache.cal_hid == cal_header.id).one_or_none()
        assert(cc is not None)
    finally:
        fits_storage_config.storage_root = save_storage_root



@pytest.mark.usefixtures("rollback")
def test_pinhole_mask(session):
    _init_gnirs(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        raw_pinhole_mask_file = 'N20181225S0052.fits'
        data_file = 'N20181225S0020.fits'

        fits_storage_config.storage_root = '/tmp'

        ensure_file(raw_pinhole_mask_file, '/tmp')
        ensure_file(data_file, '/tmp')

        iq = IngestQueueUtil(session, EmptyLogger())

        iq.ingest_file(raw_pinhole_mask_file, "", False, True)
        iq.ingest_file(data_file, "", False, True)

        df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
            .filter(DiskFile.canonical == True).one()
        header = session.query(Header).filter(Header.diskfile_id == df.id).one()
        cache_associations(session, header.id)

        df = session.query(DiskFile).filter(DiskFile.filename == raw_pinhole_mask_file)\
            .filter(DiskFile.canonical == True).one()
        cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

        cc = session.query(CalCache).filter(CalCache.obs_hid == header.id) \
            .filter(CalCache.cal_hid == cal_header.id).one_or_none()
        assert(cc is not None)
    finally:
        fits_storage_config.storage_root = save_storage_root


@pytest.mark.usefixtures("rollback")
def test_lampoff_flat(session):
    _init_gnirs(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        raw_lampoff_flat_file = 'N20190906S0075.fits'
        data_file = 'N20190907S0055.fits'

        fits_storage_config.storage_root = '/tmp'

        ensure_file(raw_lampoff_flat_file, '/tmp')
        ensure_file(data_file, '/tmp')

        iq = IngestQueueUtil(session, EmptyLogger())

        iq.ingest_file(raw_lampoff_flat_file, "", False, True)
        iq.ingest_file(data_file, "", False, True)

        df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
            .filter(DiskFile.canonical == True).one()
        header = session.query(Header).filter(Header.diskfile_id == df.id).one()
        cache_associations(session, header.id)

        df = session.query(DiskFile).filter(DiskFile.filename == raw_lampoff_flat_file)\
            .filter(DiskFile.canonical == True).one()
        cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

        cc = session.query(CalCache).filter(CalCache.obs_hid == header.id) \
            .filter(CalCache.cal_hid == cal_header.id).one_or_none()
        assert(cc is not None)
    finally:
        fits_storage_config.storage_root = save_storage_root


@pytest.mark.usefixtures("rollback")
def test_qh_flat(session):
    _init_gnirs(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        raw_qh_flat_file = 'N20190714S0237.fits'
        data_file = 'N20190714S0233.fits'

        fits_storage_config.storage_root = '/tmp'

        ensure_file(raw_qh_flat_file, '/tmp')
        ensure_file(data_file, '/tmp')

        iq = IngestQueueUtil(session, EmptyLogger())

        iq.ingest_file(raw_qh_flat_file, "", False, True)
        iq.ingest_file(data_file, "", False, True)

        df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
            .filter(DiskFile.canonical == True).one()
        header = session.query(Header).filter(Header.diskfile_id == df.id).one()
        cache_associations(session, header.id)

        df = session.query(DiskFile).filter(DiskFile.filename == raw_qh_flat_file)\
            .filter(DiskFile.canonical == True).one()
        cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

        cc = session.query(CalCache).filter(CalCache.obs_hid == header.id) \
            .filter(CalCache.cal_hid == cal_header.id).one_or_none()
        assert(cc is not None)
    finally:
        fits_storage_config.storage_root = save_storage_root



@pytest.mark.usefixtures("rollback")
def test_telluric_standard(session):
    _init_gnirs(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        raw_telluric_standard_file = 'N20190714S0127.fits'
        data_file = 'N20190714S0140.fits'

        fits_storage_config.storage_root = '/tmp'

        ensure_file(raw_telluric_standard_file, '/tmp')
        ensure_file(data_file, '/tmp')

        iq = IngestQueueUtil(session, EmptyLogger())

        iq.ingest_file(raw_telluric_standard_file, "", False, True)
        iq.ingest_file(data_file, "", False, True)

        df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
            .filter(DiskFile.canonical == True).one()
        header = session.query(Header).filter(Header.diskfile_id == df.id).one()
        cache_associations(session, header.id)

        df = session.query(DiskFile).filter(DiskFile.filename == raw_telluric_standard_file)\
            .filter(DiskFile.canonical == True).one()
        cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

        cc = session.query(CalCache).filter(CalCache.obs_hid == header.id) \
            .filter(CalCache.cal_hid == cal_header.id).one_or_none()
        assert(cc is not None)
    finally:
        fits_storage_config.storage_root = save_storage_root
