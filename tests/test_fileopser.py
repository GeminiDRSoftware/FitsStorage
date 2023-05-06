from fits_storage.server.fileopser import FileOpser
from fits_storage.logger import DummyLogger


class DummySession(object):
    def __init__(self):
        self.com = False

    def commit(self):
        self.com = True


class DummyFqe(object):
    request = '{"request": "echo", "args": {"echo": "Hello, world"}}'
    response = None


def test_reset():
    fo = FileOpser(DummySession(), DummyLogger())
    fo.fqe = 'foobar'
    fo.reset()

    assert fo.fqe is None


def test_echo():
    fo = FileOpser(DummySession(), DummyLogger())
    fqe = DummyFqe()
    fo.fileop(fqe)

    assert fo.s.com is True

    assert fo.response.ok is True
    assert fo.response.error == ''
    assert fo.response.value == 'Hello, world'
