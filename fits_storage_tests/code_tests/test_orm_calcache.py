from fits_storage.cal.orm.calcache import CalCache


def test_orm_calcache():
    # Trivial ORM functionality test
    c = CalCache(3, 2, 'bias', 1)

    assert c.obs_hid == 3
    assert c.cal_hid == 2
    assert c.caltype == 'bias'
    assert c.rank == 1
