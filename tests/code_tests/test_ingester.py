from helpers import make_empty_testing_db_env, fetch_file
from fits_storage.config import get_config

from fits_storage.db import sessionfactory
from fits_storage.logger import DummyLogger

from fits_storage.queues.orm.ingestqueueentry import IngestQueueEntry
from fits_storage.core.ingester import Ingester

from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header

def test_ingester(tmp_path):
    make_empty_testing_db_env(tmp_path)
    fsc = get_config()
    session = sessionfactory()
    logger = DummyLogger()
    filename = 'N20200127S0023.fits.bz2'

    fetch_file(filename, fsc.storage_root)

    iqe = IngestQueueEntry(filename, '')
    session.add(iqe)
    session.commit()

    ingester = Ingester(session, logger)
    ingester.ingest_file(iqe)

    df = session.query(DiskFile).filter(DiskFile.filename == filename).one()

    assert df.filename == filename
    assert df.present is True

    h = session.query(Header).filter(Header.diskfile_id == df.id).one()
    assert h.data_label == 'GN-2019B-FT-111-31-001'