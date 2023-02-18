from fits_storage.queues.orm.exportqueueentry import ExportQueueEntry
from fits_storage.queues.orm.ingestqueueentry import IngestQueueEntry


def test_basiceqe():
    eqe = ExportQueueEntry('filename', 'path', 'destination')

    assert isinstance(eqe, ExportQueueEntry)

def test_basiciqe():
    iqe = IngestQueueEntry('filename', 'path')

    assert isinstance(iqe, IngestQueueEntry)
