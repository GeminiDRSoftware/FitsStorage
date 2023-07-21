from fits_storage.core.orm.header import Header
from fits_storage.cal.orm.f2 import F2

from helpers import make_diskfile


def test_f2(tmp_path):
    diskfile = make_diskfile('S20181219S0333.fits.bz2', tmp_path)
    header = Header(diskfile)
    f2 = F2(header, diskfile.get_ad_object)

    assert (f2.disperser == 'JH_G5801')
    assert (f2.filter_name == 'Open&JH_G0809')
    assert (f2.lyot_stop == 'f/16_G5830')
    assert (f2.read_mode == '1')
    assert (f2.focal_plane_mask == '4pix-slit')
