import datetime
import os
import tempfile
import time
from filelock import FileLock

from fits_storage.queues.orm.exportqueueentry import ExportQueueEntry
from fits_storage.queues.orm.ingestqueueentry import IngestQueueEntry
from fits_storage.queues.orm.calcachequeueentry import CalCacheQueueEntry
from fits_storage.queues.orm.previewqueueentry import PreviewQueueEntry
from fits_storage.queues.orm.fileopsqueueentry import FileopsQueueEntry


def test_basiceqe():
    now = datetime.datetime.utcnow()
    onesec = datetime.timedelta(seconds=1)
    eqe = ExportQueueEntry('filename', 'path', 'destination')

    assert isinstance(eqe, ExportQueueEntry)
    assert eqe.filename == 'filename'
    assert eqe.path == 'path'
    assert eqe.destination == 'destination'
    assert eqe.inprogress is False
    assert eqe.after == eqe.added
    assert (eqe.added - now) < onesec
    assert eqe.failed is False
    assert eqe.sortkey == 'aaaaafilename'


def test_archivesortkey():
    eqe = ExportQueueEntry('filename', 'path', 'fooarchivebar')

    assert eqe.sortkey == 'zaaaafilename'


def test_basiciqe():
    now = datetime.datetime.utcnow()
    onesec = datetime.timedelta(seconds=1)
    iqe = IngestQueueEntry('filename', 'path')

    assert isinstance(iqe, IngestQueueEntry)
    assert iqe.filename == 'filename'
    assert iqe.path == 'path'
    assert iqe.inprogress is False
    assert iqe.failed is False
    assert (iqe.added - now) < onesec
    assert iqe.sortkey == 'aaaafilename'


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


def test_failed():
    iqe = IngestQueueEntry('filename', 'path')

    assert iqe.failed is False

    iqe.failed = True

    assert iqe.failed is True


def test_basicccqe():
    ccqe = CalCacheQueueEntry(1, 'filename')
    assert ccqe.obs_hid == 1
    assert ccqe.filename == 'filename'
    assert ccqe.sortkey == 'aaaafilename'
    assert ccqe.added is not None
    assert ccqe.inprogress is False
    assert ccqe.failed is False


def test_basicfqe():
    fqe = FileopsQueueEntry('[]')
    assert fqe.request == '[]'
    assert fqe.response is None
    assert fqe.added is not None
    assert fqe.sortkey is not None
    

class FakeDiskFile(object):
    id = 1
    filename = 'filename'


def test_basicpqe():
    fdf = FakeDiskFile
    pqe = PreviewQueueEntry(fdf)

    assert pqe.diskfile_id == 1
    assert pqe.filename == 'filename'
    assert pqe.sortkey == 'aaaafilename'
    assert pqe.inprogress is False
    assert pqe.failed is False


def test_filemixins():
    dataroot = tempfile.mkdtemp()
    filename = 'test.dat'
    fpfn = os.path.join(dataroot, filename)
    # fs timestamps come from a clock that is updated on an interrupt, now()
    # timestamps get corrected with a delta from that last interrupt.
    now = datetime.datetime.now()
    time.sleep(0.1)
    with open(fpfn, 'w') as f:
        f.write('hello')
    time.sleep(0.1)
    then = datetime.datetime.now()
    iqe = IngestQueueEntry(filename, '')
    iqe.storage_root = dataroot

    assert iqe.storage_root == dataroot
    assert now < iqe.filelastmod < then
    assert iqe.fullpathfilename == fpfn
    assert iqe.file_is_locked is False

    with FileLock(fpfn):
        assert iqe.file_is_locked is True

    os.unlink(fpfn)
    os.rmdir(dataroot)
