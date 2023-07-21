from fits_storage.cal.orm.gnirs import Gnirs
from fits_storage.core.orm.header import Header

from helpers import make_diskfile


def test_gnirs(tmp_path):
    data_file = 'N20180524S0117.fits'

    diskfile = make_diskfile(data_file, tmp_path)
    header = Header(diskfile)
    gnirs = Gnirs(header, diskfile.get_ad_object)

    assert(gnirs.disperser == 'MIRROR')
    assert(gnirs.filter_name == 'Open&H2_G0522')
    assert(gnirs.read_mode == 'Very Bright Objects')
    assert(gnirs.focal_plane_mask == 'Acq_G5530&Acq_G5531')
    assert(gnirs.camera == 'LongBlue_G5542')
    assert(gnirs.well_depth_setting == 'Shallow')
