import pytest

from fits_storage.cal.associate_calibrations import associate_cals, associate_cals_from_cache


@pytest.mark.usefixtures("rollback")
def test_associate_cals(session):
    # TODO build out db for a more meaningful test
    headers = list()
    cals = associate_cals(session, headers, caltype="all", recurse_level=0, full_query=False)
    assert(cals is not None)
    assert(len(cals) == 0)


@pytest.mark.usefixtures("rollback")
def test_associate_cals_from_cache(session):
    # TODO build out cache for a more meaningful test
    headers = list()
    cals = associate_cals_from_cache(session, headers, caltype="all", recurse_level=0, full_query=False)
    assert(cals is not None)
    assert(len(cals) == 0)
