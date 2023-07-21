from fits_storage.core.orm.header import Header
from fits_storage.cal.orm.niri import Niri

from .helpers import make_diskfile


def test_niri(tmp_path):
    data_file = 'N20180329S0134.fits'

    diskfile = make_diskfile(data_file, tmp_path)
    header = Header(diskfile)
    niri = Niri(header, diskfile.get_ad_object)

    assert(niri.focal_plane_mask == 'f6-cam_G5208')
    assert(niri.disperser == 'MIRROR')
    assert(niri.read_mode == 'Medium Background')
    assert(niri.filter_name == 'J_G0202')
    assert(niri.well_depth_setting == 'Shallow')
    assert(niri.data_section == 'Section(x1=0, x2=1024, y1=0, y2=1024)')
    assert(niri.camera == 'f6')
    assert(niri.focal_plane_mask == 'f6-cam_G5208')
