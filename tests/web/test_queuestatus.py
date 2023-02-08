from datetime import datetime

import pytest

import fits_storage
from fits_storage.orm.queue_error import QueueError
from fits_storage.web.api import lookup_diskfile
from fits_storage.web.publication import publication_ads
from fits_storage.web.queuestatus import queuestatus_summary, queuestatus_tb, queuestatus_update
from tests.web_helper import MockContext


@pytest.mark.usefixtures("rollback")
def test_queuestatus_summary(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.user, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.queuestatus, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    qerr = QueueError('filename', 'path', 'INGEST', 'error')
    session.add(qerr)
    session.flush()

    queuestatus_summary()
    assert(mock_context.resp.status == 200)


@pytest.mark.usefixtures("rollback")
def test_queuestatus_tb(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.user, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.queuestatus, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    qerr = QueueError('filename', 'path', 'INGEST', 'error')
    session.add(qerr)
    session.flush()

    queuestatus_tb('iq', qerr.id)

    assert(mock_context.resp.status == 200)




@pytest.mark.usefixtures("rollback")
def test_queuestatus_update(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.user, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.queuestatus, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    qerr = QueueError('filename', 'path', 'INGEST', 'error')
    session.add(qerr)
    session.flush()

    queuestatus_update()

    assert("'queue': 'iq'" in mock_context.resp.stuff)
