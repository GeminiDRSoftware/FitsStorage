from datetime import datetime

import pytest

import fits_storage
from fits_storage.orm.program import Program
from fits_storage.orm.programpublication import ProgramPublication
from fits_storage.orm.publication import Publication
from fits_storage.web.api import lookup_diskfile
from fits_storage.web.program import program_info
from tests.web_helper import MockContext


@pytest.mark.usefixtures("rollback")
def test_program_info(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context
    monkeypatch.setattr(fits_storage.web.user, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.program, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    program = Program('program_id')
    program.pi_coi_names = 'Charlie Brown, Lucy van Pelt'
    program.title = 'title'
    program.abstract = 'abstract'

    session.add(program)
    session.flush()

    program_info('program_id')

    assert(mock_context.resp.status == 200)
    assert('program_id' in mock_context.resp.stuff)
    assert('PI:<td>Charlie Brown' in mock_context.resp.stuff)
    assert('<td>Co-I(s):<td> Lucy van Pelt' in mock_context.resp.stuff)
    assert('abstract' in mock_context.resp.stuff)
    assert('<td>Title:<td>title' in mock_context.resp.stuff)
    assert ('Publications using this' not in mock_context.resp.stuff)

    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_program_info_no_pi_coi(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context

    monkeypatch.setattr(fits_storage.web.user, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.program, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    program = Program('program_id')
    program.pi_coi_names = ''
    program.title = 'title'
    program.abstract = 'abstract'

    session.add(program)
    session.flush()

    program_info('program_id')

    assert (mock_context.resp.status == 200)
    assert ('program_id' in mock_context.resp.stuff)

    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_program_info_with_publications(session, monkeypatch):
    mock_context = MockContext(session, method='GET')

    def _mock_get_context(initialize=True):
        return mock_context

    monkeypatch.setattr(fits_storage.web.user, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.program, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

    program = Program('program_id')
    program.pi_coi_names = 'Charlie Brown, Lucy van Pelt'
    program.title = 'title'
    program.abstract = 'abstract'

    session.add(program)
    session.flush()

    # How about publications?
    pub = Publication('bibcode', 'author', 'title', 2001, 'journal')
    session.add(pub)
    session.flush()
    prog_pub = ProgramPublication(program, pub)
    session.add(prog_pub)
    session.flush()

    pubs = session.query(Publication).all()
    progpubs = session.query(ProgramPublication).all()

    program_info('program_id')

    assert (mock_context.resp.status == 200)
    assert ('program_id' in mock_context.resp.stuff)
    assert ('Publications using this' in mock_context.resp.stuff)

    session.rollback()
