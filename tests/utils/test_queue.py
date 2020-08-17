import pytest
from fits_storage.utils.ingestqueue import IngestQueue, IngestQueueUtil
from fits_storage.utils.previewqueue import PreviewQueue, PreviewQueueUtil
import logging
from sqlalchemy import delete
import os

# FILES_TO_INGEST = (
#     'N20011022S128.fits.bz2',
#     'N20011022S130.fits.bz2',
#     'N20011022S130.fits.bz2',
#     'N20011022S130.fits.bz2',
#     'N20011022S130.fits.bz2',
#     'N20011022S140.fits.bz2'
#     )
from fits_storage.utils.queue import sortkey_for_filename, pop_queue, queue_length

FILES_TO_INGEST = (
    'N20191008S0458.fits',
    'N20191009S0025.fits',
    'N20191009S0089.fits',
    'N20191009S0131.fits',
    'N20191010S0096.fits',
    'N20191010S0138.fits'
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
class TestQueue:
    def test_sortkey_for_filename(self):
        assert sortkey_for_filename('asdf20200101E123b') == 'z20200101E123'
        assert sortkey_for_filename('asdf20200101S123b') == 'z20200101S123'
        assert sortkey_for_filename('unrecognizedfilename') == 'aunrecognizedfilename'

    def test_popqueue(self, session):
        # Read from empty queue
        queue_item = pop_queue(IngestQueue, session, dummy_logger)
        assert(queue_item is None)

        # Create IngestQueue item and read
        iqentry = IngestQueue("filename", "path")
        session.add(iqentry)
        assert(iqentry.inprogress is False)
        queue_item = pop_queue(IngestQueue, session, dummy_logger)
        assert(isinstance(queue_item, IngestQueue))
        # should have been set to in progress
        assert(queue_item.inprogress is True)
        session.query(IngestQueue).filter(IngestQueue.id == iqentry.id).delete()

    def test_queue_length(self, session):
        # Create IngestQueue item and read
        assert(queue_length(IngestQueue, session) == 0)
        session.add(IngestQueue("filename", "path"))
        session.add(IngestQueue("filename2", "path"))
        session.add(IngestQueue("filename3", "path"))
        assert(queue_length(IngestQueue, session) == 3)
        session.query(IngestQueue).filter(IngestQueue.filename == "filename").delete()
        session.query(IngestQueue).filter(IngestQueue.filename == "filename2").delete()
        session.query(IngestQueue).filter(IngestQueue.filename == "filename3").delete()


@pytest.mark.usefixtures("rollback")
@pytest.mark.slow
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
@pytest.mark.slow
class TestPreviewQueue:
    @pytest.yield_fixture()
    def ingest_from_queue(self, request, session, ingest_util, add_to_iq):
        cnt = 0
        while True:
            try:
                iq = ingest_util.pop()
                # some of the arguments no longer exist (skip_fv, skip_md)
                #cnt += ingest_util.ingest_file(iq.filename, iq.path, force_md5=False, force=True, skip_fv=True, skip_md=True)
                cnt += ingest_util.ingest_file(iq.filename, iq.path, force_md5=False, force=True)
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
