from fits_storage.core.orm.header import Header
from fits_storage.cal.orm.michelle import Michelle

from fits_storage_tests.code_tests.helpers import make_diskfile


def test_michelle(tmp_path):
    data_file = 'N20100119S0080.fits.bz2'

    diskfile = make_diskfile(data_file, tmp_path)
    header = Header(diskfile)
    michelle = Michelle(header, diskfile.get_ad_object)

    assert(michelle.disperser == 'MedN2')
    assert(michelle.filter_name == 'blank')
    assert(michelle.read_mode == 'stare')
    assert(michelle.focal_plane_mask == '2_pixels')
