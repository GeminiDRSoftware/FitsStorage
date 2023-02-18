from fits_storage.queues.orm.exportqueueentry import ExportQueueEntry
from fits_storage.queues.orm.ingestqueueentry import IngestQueueEntry


def test_basiceqe():
    eqe = ExportQueueEntry('filename', 'path', 'destination')

    assert isinstance(eqe, ExportQueueEntry)

def test_basiciqe():
    iqe = IngestQueueEntry('filename', 'path')

    assert isinstance(iqe, IngestQueueEntry)

def test_sortkey_facility():
    iqe = IngestQueueEntry('S20121122S9876_blah.fits', path='')

    assert iqe.sortkey == 'z201211229876'


def test_sortkey_igrins():
    iqe = IngestQueueEntry('SDCH_20121020_9876.fits', path='')

    assert iqe.sortkey == 'z201210209876'


def test_sortkey_skycam():
    iqe = IngestQueueEntry('img_20221020_12h23m34s.fits', path='')

    assert iqe.sortkey == 'y2022102012h23m34s'


def test_sortkey_obslog():
    iqe = IngestQueueEntry('20001122_GN-2000A-Q-11_obslog.txt', path='')

    assert iqe.sortkey == 'x20001122GN-2000A-Q-11'

def test_sortkey_other():
    iqe = IngestQueueEntry('blahblah', path='')

    assert iqe.sortkey == 'aaaablahblah'
