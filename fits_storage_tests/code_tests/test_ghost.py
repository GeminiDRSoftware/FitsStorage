from fits_storage.cal.orm.ghost import Ghost
from fits_storage.core.orm.header import Header

from fits_storage_tests.code_tests.helpers import get_test_config, make_diskfile


def test_ghost(tmp_path):
    get_test_config()

    data_file = 'S20240320S0027.fits.bz2'

    diskfile = make_diskfile(data_file, tmp_path)
    header = Header(diskfile)
    red = Ghost(header, diskfile.get_ad_object, arm='red')
    blue = Ghost(header, diskfile.get_ad_object, arm='red')
    sv = Ghost(header, diskfile.get_ad_object, arm='red')

    assert(red.detector_x_bin == 1)
    assert(red.detector_y_bin == 1)
    assert(red.gain_setting == 'low')
    assert(red.read_speed_setting == 'medium')
    assert(red.focal_plane_mask == 'SR')
    assert(red.prepared is False)

    assert(blue.detector_x_bin == 1)
    assert(blue.detector_y_bin == 1)
    assert(blue.gain_setting == 'low')
    assert(blue.read_speed_setting == 'medium')
    assert(blue.focal_plane_mask == 'SR')
    assert(blue.prepared is False)

    assert(sv.detector_x_bin == 1)
    assert(sv.detector_y_bin == 1)
    assert(sv.gain_setting == 'low')
    assert(sv.read_speed_setting == 'medium')
