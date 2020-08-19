import astrodata
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.file import File
from fits_storage.orm.gsaoi import Gsaoi
from fits_storage.orm.header import Header
from tests.file_helper import ensure_file
import fits_storage.fits_storage_config as fsc


def test_gsaoi(monkeypatch):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    data_file = 'S20181018S0151.fits'

    ensure_file(data_file, '/tmp')
    ad = astrodata.open('/tmp/%s' % data_file)

    f = File(data_file)
    df = DiskFile(f, data_file, "")
    df.ad_object = ad
    h = Header(df)
    gsaoi = Gsaoi(h, ad)

    assert(gsaoi.filter_name == 'K_G1106&Clear')
    assert(gsaoi.read_mode == 'FOWLER')
