import pytest

from fits_storage.orm.exportqueue import ExportQueue


def test_exportqueue():
    eq = ExportQueue("filename", "path", "url")
    assert(eq.filename == "filename")
    assert(eq.path == "path")
    assert(eq.destination == "url")
    assert(eq.inprogress is False)
    assert(eq.failed is False)


@pytest.mark.usefixtures("rollback")
def test_find_not_in_progress(session):
    eq = ExportQueue("filename", "path", "url")
    session.add(eq)
    eqs = ExportQueue.find_not_in_progress(session)
    eqsl = list(eqs)
    assert(len(eqsl) == 1)
    assert(eqsl[0].id == eq.id)
    assert(eqsl[0].filename == "filename")
    session.delete(eq)


def test_exportqueue_repr():
    eq = ExportQueue("filename", "path", "url")
    eq.id = 123
    assert(eq.__repr__() == "<ExportQueue('%s', '%s')>" % (eq.id, eq.filename))