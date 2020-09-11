import pytest

import fits_storage
from fits_storage.web.gmoscal import gmoscal
from tests.web_helper import MockContext


@pytest.mark.usefixtures("rollback")
def test_gmoscal(session, monkeypatch):
    def _mock_get_context(initialize=True):
        return MockContext(session)

    monkeypatch.setattr(fits_storage.web.gmoscal, "get_context", _mock_get_context)
    r = gmoscal({'date': "20200101"})
    assert r['said_selection'] == '; Date: 20200101'
    assert '2x2' in r['binlist']
    assert 'Full Frame' in r['roilist']
    assert 'Central Spectrum' in r['roilist']
    session.rollback()
