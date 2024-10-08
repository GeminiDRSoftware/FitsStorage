from fits_storage.core.orm.header import Header
from fits_storage.cal.orm.nifs import Nifs

from fits_storage_tests.code_tests.helpers import get_test_config, make_diskfile


def test_nifs(tmp_path):
    get_test_config()

    data_file = 'N20150505S0119.fits.bz2'

    diskfile = make_diskfile(data_file, tmp_path)
    header = Header(diskfile)
    nifs = Nifs(header, diskfile.get_ad_object)

    assert(nifs.filter_name == 'HK_G0603')
    assert(nifs.focal_plane_mask == '3.0_Mask_G5610')
    assert(nifs.disperser == 'K_G5605')
    assert(nifs.read_mode == 'Medium Object')
