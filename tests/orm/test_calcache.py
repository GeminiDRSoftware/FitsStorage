from fits_storage.orm.calcache import CalCache


def test_calcache():
    # trivial test
    cc = CalCache(1, 2, 'arc', 3)
    assert(cc.obs_hid == 1)
    assert(cc.cal_hid == 2)
    assert(cc.caltype == 'arc')
    assert(cc.rank == 3)
