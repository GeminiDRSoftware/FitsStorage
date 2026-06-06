from fits_storage.server.orm.objcat import Objcat

from fits_storage.core.orm.header import Header
from fits_storage_tests.code_tests.helpers import get_test_config, make_diskfile


def test_reduction(tmp_path):
    get_test_config()

    # We use this reduced GMOS image as a basis
    data_file = 'S20260530S0125_image.fits.bz2'

    diskfile = make_diskfile(data_file, tmp_path)
    header = Header(diskfile)

    ad = diskfile.get_ad_object

    table = ad[0].OBJCAT

    assert table is not None

    row = table[0]
    objcat = Objcat(header.id, row)

    # spot checks.
    # integer values should compare directly
    assert row['NUMBER'] == objcat.NUMBER
    assert row['X_WORLD'] - objcat.X_WORLD < 0.001

    for row in table:
        objcat = Objcat(header.id, row)

        assert objcat.NUMBER is not None


