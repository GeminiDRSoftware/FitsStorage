from fits_storage.cal.orm.gsaoi import Gsaoi
from fits_storage.core.orm.header import Header

from fits_storage_tests.code_tests.helpers import make_diskfile


def test_gsaoi(tmp_path):
    data_file = 'S20181018S0151.fits.bz2'

    diskfile = make_diskfile(data_file, tmp_path)
    header = Header(diskfile)
    gsaoi = Gsaoi(header, diskfile.get_ad_object)

    assert(gsaoi.filter_name == 'K_G1106&Clear')
    assert(gsaoi.read_mode == 'FOWLER')
