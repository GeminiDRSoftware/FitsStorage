import ssl
from http.client import IncompleteRead

from urllib.error import URLError

import urllib

from datetime import datetime

import pytest

import fits_storage
from fits_storage import fits_storage_config
from fits_storage.orm import sessionfactory
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.exportqueue import ExportQueue
from fits_storage.orm.file import File
from fits_storage.utils.exportqueue import ExportQueueUtil, get_destination_data_md5
from fits_storage.utils.previewqueue import PreviewQueueUtil
from fits_storage.utils.null_logger import EmptyLogger
from fits_storage.utils import queue

from tests.file_helper import ensure_file


def test_export_queue_util_pop(monkeypatch):
    def mock_pop_queue(queue_class, session, logger, fast_rebuild=False):
        return "test"

    monkeypatch.setattr(queue, "pop_queue", mock_pop_queue)
    session = None
    logging = EmptyLogger()
    equ = ExportQueueUtil(session, logging)
    next = equ.pop()
    assert(next == "test")


def test_export_queue_util_length(monkeypatch):
    def mock_queue_length(queue_class, session):
        return 1

    monkeypatch.setattr(queue, "queue_length", mock_queue_length)
    session = None
    logging = EmptyLogger()
    equ = ExportQueueUtil(session, logging)
    length = equ.length()
    assert(length == 1)


def test_export_queue_util_set_error(monkeypatch):
    queue_error = None

    def mock_queue_add_error(queue_class, trans, exc_type, exc_value, tb, session):
        nonlocal queue_error
        _queue_error = (queue_class, trans, exc_type, exc_value, tb, session)

    monkeypatch.setattr(queue, "add_error", mock_queue_add_error)
    equ = ExportQueueUtil('session', None)
    equ.set_error('trans', 'exc_type', 'exc_value', 'tb')
    assert(queue_error == (ExportQueue, 'trans', 'exc_type', 'exc_value', 'tb', 'session'))


def test_export_queue_util_delete(monkeypatch):
    del_id = None

    def mock_delete_with_id(self, id, session):
        nonlocal del_id
        del_id = id
    monkeypatch.setattr(queue, "delete_with_id", mock_delete_with_id)
    equ = ExportQueueUtil('session', None)
    trans = ExportQueue()
    trans.id = 123
    equ.delete(trans)
    assert(del_id == 123)


def test_export_queue_util_set_last_failed(monkeypatch):
    eq = ExportQueue('destination', 'filename', 'path')

    class MockQuery:
        def get(self, id):
            return eq

    class MockSession():
        def query(self, clazz):
            return MockQuery()

    equ = ExportQueueUtil(MockSession(), None)
    dt = datetime.now()
    equ.set_last_failed(dt)
    assert(eq.lastfailed == dt)


def test_export_queue_util_add_to_queue(monkeypatch):
    eq = ExportQueue('destination', 'filename', 'path')

    class MockQuery:
        def __init__(self):
            self.result = None

        def get(self, id):
            return eq

        def filter(self):
            return self

        def one_or_none(self):
            return self.result

    class MockSession:
        def __init__(self, query):
            self.added = None
            self.mock_query = query

        def query(self, clazz):
            return self.mock_query

        def add(self, added):
            self.added = added

        def commit(self):
            pass

    mq = MockQuery()
    ms = MockSession(mq)

    equ = ExportQueueUtil(ms, None)

    # simulate we already have the entry
    mq.result = ExportQueue('destination', 'filename', 'path')
    check = equ.add_to_queue('filename', 'path', 'destination')
    assert(check == mq.result)

    # simulate we do not have the entry
    mq.result = None
    check = equ.add_to_queue('filename', 'path', 'destination')
    assert(check == ms.added)
    assert(check.filename == 'filename')
    assert(check.path == 'path')
    assert(check.destination == 'destination')


def test_export_queue_util_export_file(monkeypatch):
    eq = ExportQueue('destination', 'filename', 'path')
    mock_diskfile = DiskFile(File('filename'), 'filename', 'path')

    class MockQuery:
        def __init__(self):
            self.result = None

        def get(self, id):
            return eq

        def filter(self):
            return self

        def one_or_none(self):
            return self.result

    class MockSession:
        def __init__(self, query):
            self.added = None
            self.mock_query = query

        def query(self, clazz):
            return self.mock_query

        def add(self, added):
            self.added = added

        def commit(self):
            pass

    class MockFile:
        def read(self):
            return 'data'

    def mock_open(filename, mode):
        return MockFile()

    data_md5 = None
    def mock_get_destination_data_md5(filename, log, destination):
        return data_md5

    class MockUrl:
        def __init__(self, filename='filename', size=5, md5='data_md5', code=200, raises=None):
            self.filename = filename
            self.size = size
            self.md5 = md5
            self.code = code
            self.raises = raises

        def read(self):
            if self.raises:
                raise self.raises()
            return '[{"filename": "%s", "size": %d, "data_md5": "%s"}]' % (self.filename, self.size, self.md5)

        def getcode(self):
            return self.code

        def close(self):
            pass

    mock_url = MockUrl()
    
    def mock_urlopen(request, timeout=None):
        return mock_url

    monkeypatch.setattr(fits_storage.utils.exportqueue, 'get_destination_data_md5', mock_get_destination_data_md5)
    monkeypatch.setattr(fits_storage.utils.exportqueue, 'open', mock_open)
    monkeypatch.setattr(urllib.request, 'urlopen', mock_urlopen)

    mq = MockQuery()
    ms = MockSession(mq)

    equ = ExportQueueUtil(ms, None)

    # error in the MD5, should return False
    data_md5 = 'ERROR'
    result = equ.export_file('filename', 'path', 'destination')
    assert(result is False)

    # matching MD5, should no-op and return True
    data_md5 = 'our_md5'
    mock_diskfile.md5 = 'our_md5'
    result = equ.export_file('filename', 'path', 'destination')
    assert(result is True)

    # different MD5, now it will do the export
    data_md5 = 'data_md5'
    result = equ.export_file('filename', 'path', 'destination')
    assert(result is True)

    # bad filename
    mock_url = MockUrl(filename='badfilename')
    result = equ.export_file('filename', 'path', 'destination')
    assert(result is False)

    # bad size
    mock_url = MockUrl(size=102)
    result = equ.export_file('filename', 'path', 'destination')
    assert(result is False)

    # bad md5
    mock_url = MockUrl(md5='badmd5')
    result = equ.export_file('filename', 'path', 'destination')
    assert(result is False)

    # bad status
    mock_url = MockUrl(status=500)
    result = equ.export_file('filename', 'path', 'destination')
    assert(result is False)

    # URLError
    mock_url = MockUrl(raises=URLError)
    result = equ.export_file('filename', 'path', 'destination')
    assert(result is False)

    # IncompleteRead
    mock_url = MockUrl(raises=IncompleteRead)
    result = equ.export_file('filename', 'path', 'destination')
    assert(result is False)

    # SSLError
    mock_url = MockUrl(raises=ssl.SSLError)
    result = equ.export_file('filename', 'path', 'destination')
    assert(result is False)


def test_export_queue_util_retry_failures(monkeypatch):
    class MockQuery:
        def __init__(self):
            self.dict = None

        def filter(self):
            return self

        def update(self, dict):
            self.dict = dict

    class MockSession:
        def __init__(self, query):
            self.added = None
            self.mock_query = query

        def query(self, clazz):
            return self.mock_query

        def commit(self):
            pass

    mq = MockQuery()
    ms = MockSession(mq)

    equ = ExportQueueUtil(ms, None)

    equ.retry_failures(42)
    assert(mq.dict == {"inprogress": False})


def test_get_destination_data_md5(monkeypatch):
    mock_data = []

    class MockUrl:
        def __init__(self, raises=None):
            self.raises = raises

        def read(self):
            if self.raises:
                raise self.raises()
            return str(mock_data)

        def close(self):
            pass

    mock_url = MockUrl()

    def mock_urlopen(request, timeout=None):
        return mock_url

    monkeypatch.setattr(urllib.request, 'urlopen', mock_urlopen)

    md5 = get_destination_data_md5('filename', 'logger', 'destination')
    assert(md5 is None)

    # ok now with 2 rows
    mock_data = [
        {"filename": "filename", "data_md5": "md5"},
        {"filename": "filename", "data_md5": "md5"},
    ]
    md5 = get_destination_data_md5('filename', 'logger', 'destination')
    assert(md5 == "ERROR")

    # Now with ok data
    mock_data = [
        {"filename": "filename", "data_md5": "md5"},
    ]
    md5 = get_destination_data_md5('filename', 'logger', 'destination')
    assert(md5 == "md5")

    # URLError
    mock_url = MockUrl(raises=URLError)
    md5 = get_destination_data_md5('filename', 'logger', 'destination')
    assert(md5 == "ERROR")

    # IncompleteRead
    mock_url = MockUrl(raises=IncompleteRead)
    md5 = get_destination_data_md5('filename', 'logger', 'destination')
    assert(md5 == "ERROR")
