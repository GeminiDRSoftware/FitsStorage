import pytest

from fits_storage.orm.calcachequeue import CalCacheQueue


def test_constructor():
    ccq = CalCacheQueue(1, 'filename', 'sortkey')
    assert(ccq.obs_hid == 1)
    assert(ccq.filename == 'filename')
    assert(ccq.sortkey == 'sortkey')
    assert(ccq.ut_datetime is not None)
    assert(ccq.inprogress is False)
    assert(ccq.failed is False)


@pytest.mark.usefixtures("rollback")
def test_find_not_in_progress(session):
    for oldccq in session.query(CalCacheQueue).filter(CalCacheQueue.filename == 'filename').all():
        session.delete(oldccq)
    ccq = CalCacheQueue(1, 'filename', 'aaasortkey')
    session.add(ccq)
    results = CalCacheQueue.find_not_in_progress(session)
    assert(results is not None)
    ccq_result = results.first()
    assert(ccq_result.obs_hid == 1)
    assert(ccq_result.filename == 'filename')
    assert(ccq_result.sortkey == 'aaasortkey')
    session.delete(ccq)
