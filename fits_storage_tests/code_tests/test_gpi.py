from fits_storage.cal.orm.gpi import Gpi
from fits_storage.core.orm.header import Header

from fits_storage_tests.code_tests.helpers import get_test_config, make_diskfile

def test_gpi(tmp_path):
    get_test_config()

    data_file = 'S20171125S0116.fits.bz2'

    diskfile = make_diskfile(data_file, tmp_path)
    header = Header(diskfile)
    gpi = Gpi(header, diskfile.get_ad_object)

    assert(gpi.filter_name == 'IFSFILT_H_G1213')
    assert(gpi.disperser == 'DISP_WOLLASTON_G6261')
    assert(gpi.focal_plane_mask == 'FPM_H_G6225')
    assert(gpi.pupil_mask == 'APOD_H_G6205')
    assert(gpi.astrometric_standard is False)
    assert(gpi.wollaston is True)
    assert(gpi.prism is False)
