from fits_storage.core.orm.header import Header
from fits_storage.cal.orm.nici import Nici

from helpers import make_diskfile


def test_nici(tmp_path):
    data_file = 'S20130123S0131.fits'

    diskfile = make_diskfile(data_file, tmp_path)
    header = Header(diskfile)
    nici = Nici(header, diskfile.get_ad_object)

    assert(nici.filter_name == 'CH4-H4%L_G0740+CH4-H4%S_G0743')
    assert(nici.focal_plane_mask is None)
    assert(nici.disperser is None)


def test_nici_cal(tmp_path):
    data_file = 'S20130124S0036.fits'

    diskfile = make_diskfile(data_file, tmp_path)
    header = Header(diskfile)
    nici = Nici(header, diskfile.get_ad_object)

    assert(nici.filter_name == 'Kprime_G0706+CH4-H4%S_G0743')
    assert(nici.focal_plane_mask is None)
    assert(nici.disperser is None)