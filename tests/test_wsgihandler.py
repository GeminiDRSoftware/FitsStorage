import pytest

import fits_storage
from fits_storage.utils.web import RequestRedirect, ClientError
from fits_storage.wsgihandler import handler
from tests.web_helper import MockContext


@pytest.mark.usefixtures("rollback")
def test_handler(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context

    monkeypatch.setattr(fits_storage.wsgihandler, "get_context", _mock_get_context)

    excpt = None

    def handle_with_static(env, start_response):
        if excpt is not None:
            raise excpt
        return 'handle_with_static'

    monkeypatch.setattr(fits_storage.wsgihandler, 'handle_with_static', handle_with_static)

    env = None
    start_response = None

    ret = handler(env, start_response)
    assert(ret == 'handle_with_static')

    excpt = RequestRedirect()
    handler(env, start_response)
    assert(mock_context.resp.stuff == 'respond called')

    mock_context.resp.stuff = None
    excpt = ClientError(0, '')
    handler(env, start_response)
    assert (mock_context.resp.stuff == 'respond called')

    pass
