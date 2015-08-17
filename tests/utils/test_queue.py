import pytest
from fits_storage.utils.ingestqueue import IngestQueue, IngestQueueUtil
from fits_storage.utils.previewqueue import PreviewQueue, PreviewQueueUtil
import logging
from sqlalchemy import delete
import os

FILES_TO_INGEST = (
    'N20011022S128.fits.bz2',
    'N20011022S130.fits.bz2',
    'N20011022S130.fits.bz2',
    'N20011022S130.fits.bz2',
    'N20011022S130.fits.bz2',
    'N20011022S140.fits.bz2'
    )

# import sys
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
# dummy_logger = logging.getLogger()
dummy_logger = logging.getLogger('dummy')

@pytest.yield_fixture(scope="module")
def ingest_util(request, session):
    yield IngestQueueUtil(session, dummy_logger)
    session.commit()

@pytest.yield_fixture(scope="module")
def preview_util(request, session):
    yield PreviewQueueUtil(session, dummy_logger)
    session.commit()

@pytest.yield_fixture()
def add_to_iq(request, session, ingest_util, testfile_path):
    nobz2lst = []
    for f in FILES_TO_INGEST:
        path = testfile_path(f)
        filename = os.path.basename(path)
        ingest_util.add_to_queue(filename, os.path.dirname(path))
        nobz2lst.append(filename)

    yield nobz2lst

    session.execute(delete(IngestQueue))
    session.commit()

@pytest.mark.usefixtures("rollback")
class TestIngestQueue:
    def test_ingestqueue_length(self, ingest_util, add_to_iq):
        assert ingest_util.length() == len(add_to_iq)

    def test_ingestqueue_pop(self, ingest_util, add_to_iq):
        files = set(add_to_iq)
        while ingest_util.length() > 0:
            obj = ingest_util.pop()
            files.remove(obj.filename)
        assert len(files) == 0

@pytest.mark.usefixtures("rollback")
class TestPreviewQueue:
    @pytest.yield_fixture()
    def ingest_from_queue(self, request, session, ingest_util, add_to_iq):
        cnt = 0
        while True:
            try:
                iq = ingest_util.pop()
                cnt += ingest_util.ingest_file(iq.filename, iq.path, force_md5=False, force=True, skip_fv=True, skip_md=True)
            except AttributeError:
                break

        yield cnt

        session.execute(delete(PreviewQueue))
        session.commit()

    def test_previewqueue_pop(self, preview_util, ingest_from_queue):
        cnt = 0
        while True:
            if preview_util.pop() is None:
                break
            cnt += 1
        assert cnt == ingest_from_queue
