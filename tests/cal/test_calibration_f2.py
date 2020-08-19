
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


def _init_f2(session):
    session.rollback()


# TODO convert tests into fixtures or do some matching-logic specific mocking and test negatives as well


@pytest.mark.usefixtures("rollback")
def test_dark(session):
    _init_f2(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        raw_dark_file = 'S20181231S0247.fits'
        data_file = 'S20181224S0078.fits'

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
    _init_f2(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        raw_flat_file = 'S20181231S0120.fits'
        data_file = 'S20181219S0333.fits'

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
    _init_f2(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        raw_arc_file = 'S20181231S0100.fits'
        data_file = 'S20181231S0093.fits'

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

