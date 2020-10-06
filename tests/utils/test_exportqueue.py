import json

import ssl
from http.client import IncompleteRead

from urllib.error import URLError

import urllib

from datetime import datetime, timedelta

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
from tests.log_helper import DummyLogger


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
        queue_error = (queue_class, trans, exc_type, exc_value, tb, session)

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
    trans = ExportQueue('filename', 'path', 'destination')
    trans.id = 123
    equ.delete(trans)
    assert(del_id == 123)


def test_export_queue_util_set_last_failed(monkeypatch):
    eq = ExportQueue('filename', 'path', 'destination')

    class MockQuery:
        def get(self, id):
            return eq

    class MockSession():
        def query(self, clazz):
            return MockQuery()

        def commit(self):
            pass

    equ = ExportQueueUtil(MockSession(), None)
    equ.set_last_failed(eq)
    assert(eq.lastfailed is not None)


def test_export_queue_util_add_to_queue(monkeypatch):
    eq = ExportQueue('filename', 'path', 'destination')

    class MockQuery:
        def __init__(self):
            self.result = None

        def get(self, id):
            return eq

        def filter(self, term):
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

    equ = ExportQueueUtil(ms, DummyLogger())

    # simulate we already have the entry
    mq.result = ExportQueue('filename', 'path', 'destination')
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
    eq = ExportQueue('filename', 'path', 'destination')

    file_md5 = 'md5'

    def mock_get_file_size(df):
        return 4

    def mock_get_file_md5(df):
        return file_md5

    def mock_get_lastmod(df):
        return datetime.now()

    monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, 'get_file_size', mock_get_file_size)
    monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, 'get_file_md5', mock_get_file_md5)
    monkeypatch.setattr(fits_storage.orm.diskfile.DiskFile, 'get_lastmod', mock_get_lastmod)
    # turn off bzip
    monkeypatch.setattr(fits_storage.utils.exportqueue, 'export_bzip', False)

    mock_diskfile = DiskFile(File('filename'), 'filename', 'path')

    class MockQuery:
        def __init__(self):
            self.result = mock_diskfile

        def get(self, id):
            return eq

        def select_from(self, *args):
            return self

        def filter(self, term):
            return self

        def one(self):
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
        def __init__(self, filename='filename', size=4, md5='data_md5', code=200, raises_err=None):
            self.filename = filename
            self.size = size
            self.md5 = md5
            self.code = code
            self.raises = raises_err
            self.call = 0

        def read(self):
            if self.raises:
                raise self.raises()
            md5 = self.md5
            if not isinstance(md5, str):
                md5 = md5[self.call % len(md5)]
            return '[{"filename": "%s", "size": %d, "md5": "%s"}]' % (self.filename, self.size, md5)
            self.call = self.call + 1

        def getcode(self):
            return self.code

        def close(self):
            pass

    mock_url = MockUrl()

    def mock_urlopen(request, timeout=None):
        return mock_url

    class MockS3Item:
        def get_contents_as_string(self):
            return 'data'.encode('utf-8')

    class MockS3Helper:
        def exists_key(self, key):
            return True

        def get_as_string(self, key):
            return MockS3Item()

    def mock_get_helper():
        return MockS3Helper()

    monkeypatch.setattr(fits_storage.utils.exportqueue, 'get_destination_data_md5', mock_get_destination_data_md5)
    monkeypatch.setattr(urllib.request, 'urlopen', mock_urlopen)

    # have to hack in s3 path because we don't want to monkeypatch the open builtin (can break pytest)
    monkeypatch.setattr(fits_storage.utils.exportqueue, 'using_s3', True)
    # monkeypatch.setattr(fits_storage.utils.aws_s3, 'get_helper', mock_get_helper)
    fits_storage.utils.exportqueue.get_helper = mock_get_helper

    mq = MockQuery()
    ms = MockSession(mq)

    equ = ExportQueueUtil(ms, DummyLogger())

    # error in the MD5, should return False
    data_md5 = 'ERROR'
    result = equ.export_file('filename', 'path', 'http://destination')
    assert(result is False)

    # matching MD5, should no-op and return True
    data_md5 = 'our_md5'
    mock_diskfile.data_md5 = 'our_md5'
    result = equ.export_file('filename', 'path', 'http://destination')
    assert(result is True)

    # different MD5, now it will do the export
    data_md5 = 'data_md5'
    mock_url = MockUrl(md5='our_md5')
    result = equ.export_file('filename', 'path', 'http://destination')
    assert(result is True)

    # bad filename
    mock_url = MockUrl(filename='badfilename', md5='our_md5')
    result = equ.export_file('filename', 'path', 'http://destination')
    assert(result is False)

    # bad size
    mock_url = MockUrl(size=102, md5='our_md5')
    result = equ.export_file('filename', 'path', 'http://destination')
    assert(result is False)

    # bad md5
    mock_url = MockUrl(md5='badmd5')
    result = equ.export_file('filename', 'path', 'http://destination')
    assert(result is False)

    # bad status
    mock_url = MockUrl(code=500, md5='our_md5')
    result = equ.export_file('filename', 'path', 'http://destination')
    assert(result is False)

    # URLError
    mock_url = MockUrl(raises_err=lambda: URLError(reason=None), md5='our_md5')
    result = equ.export_file('filename', 'path', 'http://destination')
    assert(result is False)

    # IncompleteRead
    mock_url = MockUrl(raises_err=lambda: IncompleteRead(partial=''), md5='our_md5')
    result = equ.export_file('filename', 'path', 'http://destination')
    assert(result is False)

    # SSLError
    mock_url = MockUrl(raises_err=ssl.SSLError, md5='our_md5')
    result = equ.export_file('filename', 'path', 'http://destination')
    assert(result is False)


def test_export_queue_util_retry_failures(monkeypatch):
    class MockQuery:
        def __init__(self):
            self.dict = None

        def filter(self, term):
            return self

        def update(self, dict):
            self.dict = dict
            return 1

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

    equ = ExportQueueUtil(ms, DummyLogger())

    equ.retry_failures(timedelta(days=1))
    assert(mq.dict == {"inprogress": False})


def test_get_destination_data_md5(monkeypatch):
    mock_data = []

    class MockUrl:
        def __init__(self, raises=None):
            self.raises = raises

        def read(self):
            if self.raises:
                raise self.raises()
            return json.dumps(mock_data)

        def close(self):
            pass

    mock_url = MockUrl()

    def mock_urlopen(request, timeout=None):
        return mock_url

    monkeypatch.setattr(urllib.request, 'urlopen', mock_urlopen)

    md5 = get_destination_data_md5('filename', DummyLogger(), 'destination')
    assert(md5 is None)

    # ok now with 2 rows
    mock_data = [
        {"filename": "filename", "data_md5": "md5"},
        {"filename": "filename", "data_md5": "md5"},
    ]
    md5 = get_destination_data_md5('filename', DummyLogger(), 'destination')
    assert(md5 == "ERROR")

    # Now with ok data
    mock_data = [
        {"filename": "filename", "data_md5": "md5"},
    ]
    md5 = get_destination_data_md5('filename', DummyLogger(), 'destination')
    assert(md5 == "md5")

    # URLError
    mock_url = MockUrl(raises=lambda: URLError(reason=None))
    md5 = get_destination_data_md5('filename', DummyLogger(), 'destination')
    assert(md5 == "ERROR")

    # IncompleteRead
    mock_url = MockUrl(raises=lambda: IncompleteRead(partial=''))
    md5 = get_destination_data_md5('filename', DummyLogger(), 'destination')
    assert(md5 == "ERROR")
