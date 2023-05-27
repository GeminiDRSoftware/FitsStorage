import datetime
import astrodata

from fits_storage.server.fileopser import FileOpser
from fits_storage.queues.orm.fileopsqueueentry import FileopsQueueEntry

from fits_storage.queues.queue.fileopsqueue import \
    FileOpsRequest, FileOpsResponse

from fits_storage.server.orm.usagelog import UsageLog
from fits_storage.server.orm.fileuploadlog import FileUploadLog

from fits_storage.queues.orm.ingestqueueentry import IngestQueueEntry

from fits_storage.logger import DummyLogger
from fits_storage.core.ingester import Ingester
from fits_storage.core.orm.diskfile import DiskFile

from helpers import make_empty_testing_db_env, fetch_file
from fits_storage.config import get_config

from fits_storage.db import sessionfactory

class DummySession(object):
    def __init__(self):
        self.com = False

    def commit(self):
        self.com = True


class DummyFqe(object):
    request = '{"request": "echo", "args": {"echo": "Hello, world"}}'
    response_required = True
    response = None


def test_reset():
    fo = FileOpser(DummySession(), DummyLogger())
    fo.fqe = 'foobar'
    fo.reset()

    assert fo.fqe is None


def test_echo_nodb():
    fo = FileOpser(DummySession(), DummyLogger())
    fqe = DummyFqe()
    fo.fileop(fqe)

    assert fo.s.com is True

    assert fo.response.ok is True
    assert fo.response.error == ''
    assert fo.response.value == 'Hello, world'

def test_echo(tmp_path):
    make_empty_testing_db_env(tmp_path)
    fsc = get_config()
    session = sessionfactory()
    logger = DummyLogger()

    fqreq = FileOpsRequest(request='echo', args={"echo": "Hello, world"})
    fqe = FileopsQueueEntry(fqreq.json(), response_required=True)

    session.add(fqe)
    session.commit()

    fo = FileOpser(session, logger)
    fo.fileop(fqe)

    assert fo.response.ok is True
    assert fo.response.error == ''
    assert fo.response.value == 'Hello, world'

    fqe_fetch = session.query(FileopsQueueEntry).one()
    resp_fetch = FileOpsResponse()
    resp_fetch.loads(fqe_fetch.response)

    assert resp_fetch.ok is True
    assert resp_fetch.error == ''
    assert resp_fetch.value == 'Hello, world'

    session.commit()


def test_ingest_upload(tmp_path):
    make_empty_testing_db_env(tmp_path)
    fsc = get_config()
    session = sessionfactory()
    logger = DummyLogger()
    filename = 'N20200127S0023.fits.bz2'

    ul = UsageLog(None)
    session.add(ul)
    session.commit()
    ful = FileUploadLog(ul)
    session.add(ful)
    session.commit()

    fetch_file(filename, fsc.upload_staging_dir)
    args = {'filename': filename,
            'fileuploadlog_id': ful.id,
            'processed_cal': False}

    fqreq = FileOpsRequest(request='ingest_upload', args=args)
    fqe = FileopsQueueEntry(fqreq.json(), response_required=True)

    session.add(fqe)
    session.commit()

    before_ut = datetime.datetime.utcnow()
    fo = FileOpser(session, logger)
    fo.fileop(fqe)
    session.commit()
    after_ut = datetime.datetime.utcnow()

    assert fo.response.ok is True
    assert fo.response.error == ''
    assert fo.response.value is True

    fqe_fetch = session.query(FileopsQueueEntry).one()
    resp_fetch = FileOpsResponse()
    resp_fetch.loads(fqe_fetch.response)

    assert resp_fetch.ok is True
    assert resp_fetch.error == ''
    assert resp_fetch.value is True

    # Check we did actually queue the file for ingest
    iqe = session.query(IngestQueueEntry).\
        filter(IngestQueueEntry.filename == filename).one()

    assert iqe.filename == filename
    assert iqe.inprogress is False
    assert iqe.failed is False
    assert iqe.added > before_ut
    assert iqe.added < after_ut

def test_update_headers(tmp_path):
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

    # Check the ingest worked
    df = session.query(DiskFile).filter(DiskFile.filename == filename).one()
    assert df.filename == filename
    assert df.present is True

    # Sanity check initial headers
    ad = astrodata.open(df.fullpath)
    assert ad.qa_state() == 'Usable'
    assert ad.phu['RELEASE'] == '2020-07-27'
    assert ad.phu['RAWIQ'] == '70-percentile'
    assert ad.phu['RAWCC'] == '50-percentile'
    assert ad.phu['SSA'] == 'B. Cooper'
    assert 'KANGAROO' not in ad.phu.keys()

    # OK, go ahead and set up the fileops request
    args = {'filename': filename,
            'qa_state': 'Fail',
            'release': '2025-01-23',
            'rawsite': ['iqany', 'cc80'],
            'generic': {'KANGAROO': 'Jumpy', 'SSA': 'Mickey Mouse'},
            }

    fqreq = FileOpsRequest(request='update_headers', args=args)
    fqe = FileopsQueueEntry(fqreq.json(), response_required=True)
    session.add(fqe)
    session.commit()

    fo = FileOpser(session, logger)
    fo.fileop(fqe)
    session.commit()

    assert fo.response.ok is True
    assert fo.response.error == ''
    assert fo.response.value is None

    fqe_fetch = session.query(FileopsQueueEntry).one()
    resp_fetch = FileOpsResponse()
    resp_fetch.loads(fqe_fetch.response)

    assert resp_fetch.ok is True
    assert resp_fetch.error == ''
    assert resp_fetch.value is None


    # Check final headers
    ad = astrodata.open(df.fullpath)
    assert ad.qa_state() == 'Fail'
    assert ad.phu['RELEASE'] == '2025-01-23'
    assert ad.phu['RAWIQ'] == 'Any'
    assert ad.phu['RAWCC'] == '80-percentile'
    assert ad.phu['SSA'] == 'Mickey Mouse'
    assert ad.phu['KANGAROO'] == 'Jumpy'
