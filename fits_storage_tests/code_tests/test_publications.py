from fits_storage_tests.code_tests.helpers import get_test_config
from fits_storage_tests.code_tests.helpers import make_empty_testing_db_env

from fits_storage.server.orm.program import Program
from fits_storage.server.orm.publication import Publication, ProgramPublication
get_test_config()

from fits_storage.db import sessionfactory


def test_orm_programpublication():
    # Sanity check the ORM objects for Program, Publication, and the
    # ProgramPublication association table
    pub = Publication('FOOBAR')
    pub.id = 123
    assert pub.bibcode == 'FOOBAR'

    d = {'id': 'GN-2345A-Q-678'}
    prog = Program(d)
    prog.id = 456
    assert prog.program_id == 'GN-2345A-Q-678'

    pp = ProgramPublication(prog, pub)
    assert pp.program_id == 456
    assert pp.publication_id == 123


def test_db_programpublication(tmp_path):
    make_empty_testing_db_env(tmp_path)
    session = sessionfactory()

    # Sanity check database operations of Publication
    pub = Publication('FOOBAR')
    session.add(pub)
    session.commit()
    assert pub.id is not None

    # Sanity check database operations of Program
    d = {'id': 'GN-2345A-Q-678'}
    prog = Program(d)
    session.add(prog)
    session.commit()
    assert prog.id is not None

    # Sanity check database operations of ProgramPublication
    pp = ProgramPublication(prog, pub)
    session.add(pp)
    session.commit()
    assert pp.program_id == prog.id
    assert pp.publication_id == pub.id

    # Test relationships
    assert pub in prog.publications
    assert prog in pub.programs

    # Test adding an association via the publications relationship
    pub2 = Publication('ANOTHER')
    prog.publications.add(pub2)
    session.commit()
    assert pub2 in prog.publications

    # Check that that new publication actually went into the DB
    pub2t = session.query(Publication)\
        .filter(Publication.bibcode == 'ANOTHER').one()
    assert pub2t.id == pub2.id

    # Check that that new association actually went into the DB
    ppt = session.query(ProgramPublication)\
        .filter(ProgramPublication.program_id == prog.id)\
        .filter(ProgramPublication.publication_id == pub2.id)\
        .one()
    assert ppt is not None

    # Add another program and associate it with pub2...
    # duplicate publication entries
    d = {'id': 'GN-2345A-Q-123'}
    prog2 = Program(d)
    session.add(prog2)
    session.commit()
    assert prog2.id is not None

    prog2.publications.add(pub2)
    session.commit()

    # Check that we didn't get a duplicate publication entry.
    for p in prog2.publications:
        assert p.id == pub2.id
