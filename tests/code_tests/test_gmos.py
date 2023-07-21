from fits_storage.cal.orm.gmos import Gmos
from fits_storage.core.orm.header import Header

from helpers import make_diskfile


def test_gmos(tmp_path):
    data_file = 'N20191002S0080.fits'

    diskfile = make_diskfile(data_file, tmp_path)
    header = Header(diskfile)
    gmos = Gmos(header, diskfile.get_ad_object)

    assert(gmos.disperser == 'R400+_G5305')
    assert(gmos.filter_name == 'OG515_G0306&open2-8')
    assert(gmos.detector_x_bin == 2)
    assert(gmos.detector_y_bin == 2)
    assert(gmos.gain_setting == 'low')
    assert(gmos.read_speed_setting == 'slow')
    assert(gmos.focal_plane_mask == '5.0arcsec')
    assert(gmos.nodandshuffle is False)
    assert(gmos.nod_count is None)
    assert(gmos.nod_pixels is None)
    assert(gmos.grating_order is None)
    assert(gmos.prepared is False)
    assert(gmos.overscan_trimmed is False)
    assert(gmos.overscan_subtracted is False)

