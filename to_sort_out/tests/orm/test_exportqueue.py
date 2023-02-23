import pytest

from fits_storage.orm.exportqueue import ExportQueue, _sortkey


def test_exportqueue():
    eq = ExportQueue("filename", "path", "url")
    assert(eq.filename == "filename")
    assert(eq.path == "path")
    assert(eq.destination == "url")
    assert(eq.inprogress is False)
    assert(eq.failed is False)


@pytest.mark.usefixtures("rollback")
def test_find_not_in_progress(session):
    eq = ExportQueue("S20211222S0078.fits", "path", "url")
    session.add(eq)
    eqs = ExportQueue.find_not_in_progress(session)
    eqsl = list(eqs)
    assert(len(eqsl) == 1)
    assert(eqsl[0].id == eq.id)
    assert(eqsl[0].filename == "S20211222S0078.fits")
    assert(eqsl[0].sortkey == '20211222')
    session.delete(eq)


def test_exportqueue_repr():
    eq = ExportQueue("filename", "path", "url")
    eq.id = 123
    assert(eq.__repr__() == "<ExportQueue('%s', '%s')>" % (eq.id, eq.filename))


def test_sortkey():
    assert _sortkey('S20211222S0078.fits') == '20211222'
    assert _sortkey('N20190121S0078.fits') == '20190121'
    assert _sortkey('20150608_GS-2015A-Q-66_obslog.txt') == '20150608'
    assert _sortkey('img_20170720_06h06m58s.fits') == '20170720'
    assert _sortkey('SDCH_20210506_0020.fits') == '20210506'
    assert _sortkey('SDCK_20210501_0064.fits') == '20210501'
    assert _sortkey('filename.fits') == '0000filename.fits'
