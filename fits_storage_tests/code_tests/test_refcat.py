from fits_storage.server.orm.refcat import Refcat

from fits_storage.core.orm.header import Header
from fits_storage_tests.code_tests.helpers import get_test_config, make_diskfile


def test_refcat(tmp_path):
    get_test_config()

    # We use this reduced GMOS image as a basis
    data_file = 'N20260921S0214_reducedFrame.fits.bz2'

    diskfile = make_diskfile(data_file, tmp_path)
    header = Header(diskfile)

    ad = diskfile.get_ad_object

    table = ad.REFCAT

    assert table is not None

    row = table[0]
    refcat = Refcat(header.id, row)

    # spot checks.
    # integer values should compare directly
    assert row['Cat_Id'] == refcat.Cat_Id
    assert row['rmag'] - refcat.rmag < 0.001

    for row in table:
        refcat = Refcat(header.id, row)

        assert refcat.refcatid is not None


