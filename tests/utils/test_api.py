import urllib

import pytest

from fits_storage.orm.notification import Notification
from fits_storage.utils.api import ApiProxy, HTTP_OK, BAD_REQUEST, FORBIDDEN, NOT_FOUND, METHOD_NOT_ALLOWED, \
    INTERNAL_ERROR, get_status_text
from fits_storage.utils.notifications import ingest_odb_xml


_mock_response = None
_saw_path = None
_saw_data = None


class MockResponse():
    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data


def mock_urlopen(path, data):
    global _saw_path
    global _saw_data
    _saw_path = path
    _saw_data = data
    return _mock_response


def test_api_proxy(monkeypatch):
    monkeypatch.setattr(urllib.request, 'urlopen', mock_urlopen)

    proxy = ApiProxy('host', 'prefix')
    global _mock_response
    _mock_response = MockResponse('{"result": "response"}')

    result = proxy.__invoke('action', foo='bar')
    assert(_saw_path == 'http://host:80/prefix/__invoke')
    assert(_saw_data == b'{"foo": "bar"}')
    assert(result == 'response')


_status_tests = [
    (HTTP_OK, "200 OK"),
    (BAD_REQUEST, "400 Bad Request"),
    (FORBIDDEN, "403 Forbidden"),
    (NOT_FOUND, "404 Not Found"),
    (METHOD_NOT_ALLOWED, "405 Method not Allowed"),
    (INTERNAL_ERROR, "500 Internal Error"),
]


@pytest.mark.parametrize("status,expected", _status_tests)
def test_get_status_text(status, expected):
    assert(get_status_text(status) == expected)
