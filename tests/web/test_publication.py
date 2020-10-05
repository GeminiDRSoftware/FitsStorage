from datetime import datetime

import pytest

import fits_storage
from fits_storage.orm.publication import Publication
from fits_storage.web.api import lookup_diskfile
from fits_storage.web.publication import publication_ads, list_publications
from tests.web_helper import MockContext


@pytest.mark.usefixtures("rollback")
def test_publication_ads(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.publication, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    publication_ads(None)
    assert(mock_context.resp.status == 404)  # bibcode is invalid
    publication_ads('bibcode')
    assert(mock_context.resp.status == 404)  # bibcode isn't in database

    p = Publication('bibcode', 'author', 'title', 2001, 'Journal of Irreproduceable Results')
    session.add(p)
    session.flush()

    publication_ads('bibcode')

    assert(mock_context.resp.status == 200)
    assert(mock_context.resp.stuff == '%R bibcode\n%A author\n%J Journal of Irreproduceable Results\n%D 2001\n%T title')
    assert(mock_context.resp.content_type == 'text/plain')


@pytest.mark.usefixtures("rollback")
def test_list_publications(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.publication, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    list_publications()
    assert(mock_context.resp.status == 200)
    assert(mock_context.resp.stuff == '\nbibcode   https://archive.gemini.edu/searchform/bibcode=bibcode\n')
