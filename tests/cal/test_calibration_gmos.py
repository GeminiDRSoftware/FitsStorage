
import pytest
from fits_storage.cal import get_cal_object
from gemini_obs_db.calcache import CalCache
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.header import Header
from fits_storage.utils.calcachequeue import CalCacheQueueUtil, cache_associations
from fits_storage.utils.ingestqueue import IngestQueueUtil
from fits_storage.utils.null_logger import EmptyLogger

from fits_storage import fits_storage_config as fsc
from tests.file_helper import ensure_file


def _mock_sendmail(fromaddr, toaddr, message):
    pass


def _init_gmos(session):
    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_arc(monkeypatch, session):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    _init_gmos(session)

    raw_arc_file = 'N20181113S0262.fits'
    data_file = 'N20191002S0080.fits'

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


@pytest.mark.usefixtures("rollback")
def test_dark(monkeypatch, session):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    _init_gmos(session)

    raw_dark_file = 'N20180329S0376.fits'
    data_file = 'N20180329S0134.fits'

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


@pytest.mark.usefixtures("rollback")
def test_bias(monkeypatch, session):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    _init_gmos(session)

    raw_bias_file = 'N20180122S0002.fits'
    data_file = 'N20180117S0078.fits'

    ensure_file(raw_bias_file, '/tmp')
    ensure_file(data_file, '/tmp')

    iq = IngestQueueUtil(session, EmptyLogger())

    iq.ingest_file(raw_bias_file, "", False, True)
    iq.ingest_file(data_file, "", False, True)

    df = session.query(DiskFile).filter(DiskFile.filename == data_file)\
        .filter(DiskFile.canonical == True).one()
    header = session.query(Header).filter(Header.diskfile_id == df.id).one()
    cache_associations(session, header.id)

    df = session.query(DiskFile).filter(DiskFile.filename == raw_bias_file)\
        .filter(DiskFile.canonical == True).one()
    cal_header = session.query(Header).filter(Header.diskfile_id == df.id).one()

    cc = session.query(CalCache).filter(CalCache.obs_hid == header.id) \
        .filter(CalCache.cal_hid == cal_header.id).one_or_none()
    assert(cc is not None)


@pytest.mark.usefixtures("rollback")
def test_spectral_flat(monkeypatch, session):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    _init_gmos(session)

    raw_flat_file = 'N20180101S0157.fits'
    data_file = 'N20180101S0122.fits'

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


@pytest.mark.usefixtures("rollback")
def test_imaging_flat(monkeypatch, session):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    _init_gmos(session)
    raw_flat_file = 'N20180330S0199.fits'
    data_file = 'N20180317S0019.fits'

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


@pytest.mark.usefixtures("rollback")
def test_processed_fringe(monkeypatch, session):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    _init_gmos(session)

    raw_flat_file = 'N20110313S0188_fringe.fits'
    data_file = 'N20110311S0296.fits'

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
