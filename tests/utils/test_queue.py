import pytest
from fits_storage.utils.ingestqueue import IngestQueue, add_to_ingestqueue, ingest_file
from fits_storage.utils.ingestqueue import pop_ingestqueue, ingestqueue_length

from fits_storage.utils.previewqueue import PreviewQueue
from fits_storage.utils.previewqueue import pop_previewqueue
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

@pytest.yield_fixture()
def add_to_iq(request, session, testfile_path):
    nobz2lst = []
    for f in FILES_TO_INGEST:
        path = testfile_path(f)
        filename = os.path.basename(path)
        add_to_ingestqueue(session, dummy_logger, filename, os.path.dirname(path))
        nobz2lst.append(filename)

    yield nobz2lst

    session.execute(delete(IngestQueue))
    session.commit()

@pytest.mark.usefixtures("rollback")
class TestIngestQueue:
    def test_ingestqueue_length(self, session, add_to_iq):
        assert ingestqueue_length(session) == len(add_to_iq)

    def test_ingestqueue_pop(self, session, add_to_iq):
        files = set(add_to_iq)
        while ingestqueue_length(session) > 0:
            obj = pop_ingestqueue(session, dummy_logger)
            files.remove(obj.filename)
        assert len(files) == 0

@pytest.mark.usefixtures("rollback")
class TestPreviewQueue:
    @pytest.yield_fixture()
    def ingest_from_queue(self, request, session, add_to_iq):
        cnt = 0
        while True:
            try:
                iq = pop_ingestqueue(session, dummy_logger)
                cnt += ingest_file(session, dummy_logger, iq.filename, iq.path, force_md5=False, force=True, skip_fv=True, skip_md=True)
            except AttributeError:
                break

        yield cnt

        session.execute(delete(PreviewQueue))
        session.commit()

    def test_previewqueue_pop(self, session, ingest_from_queue):
        cnt = 0
        while True:
            if pop_previewqueue(session, dummy_logger) is None:
                break
            cnt += 1
        assert cnt == ingest_from_queue
