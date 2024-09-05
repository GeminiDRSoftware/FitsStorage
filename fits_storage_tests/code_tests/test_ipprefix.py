import pytest

from fits_storage_tests.code_tests.helpers import make_empty_pg_testing_db

from fits_storage.config import get_config

from fits_storage.server.orm.ipprefix import IPPrefix

# In order to actually run this, you need to swap the comment and active lines
# below, and set your local configuration to use a postgres DB, not sqlite
#fsc = get_config(builtinonly=True, reload=True)
fsc = get_config()

@pytest.mark.skipif(fsc.using_sqlite is True,
                    reason='Cannot test IPPrefix functionality using sqlite')
def test_ipprefix_sanity():
    ipp = IPPrefix()
    assert ipp.deny is False
    assert ipp.allow is False
    assert ipp.badness == 0

    ipp.prefix = '1.2.0.0/16'
    assert ipp.prefix is not None


@pytest.mark.skipif(fsc.using_sqlite is True,
                    reason='Cannot test IPPrefix functionality using sqlite')
def test_ipprefix():
    session = make_empty_pg_testing_db()
    ipp1 = IPPrefix()
    ipp1.prefix = '1.1.0.0/16'
    ipp1.name = 'FitsStorage code test 1.1.0.0/16'
    session.add(ipp1)
    ipp2 = IPPrefix()
    ipp2.prefix = '1.2.0.0/16'
    ipp2.name = 'FitsStorage code test 1.2.0.0/16'
    session.add(ipp2)
    session.commit()

    ip = '1.1.1.1'
    t = session.query(IPPrefix).filter(IPPrefix.prefix.op('>>')(ip)).all()
    assert len(t) == 1
    assert t[0].id == ipp1.id

    ip = '1.2.1.1'
    t = session.query(IPPrefix).filter(IPPrefix.prefix.op('>>')(ip)).all()
    assert len(t) == 1
    assert t[0].id == ipp2.id

    ip = '1.3.1.1'
    t = session.query(IPPrefix).filter(IPPrefix.prefix.op('>>')(ip)).all()
    assert len(t) == 0
