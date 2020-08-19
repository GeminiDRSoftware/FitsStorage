import astrodata
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.file import File
from fits_storage.orm.header import Header
from fits_storage.orm.michelle import Michelle
from tests.file_helper import ensure_file
import fits_storage.fits_storage_config as fsc


def test_michelle(monkeypatch):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    data_file = 'N20100119S0080.fits'

    ensure_file(data_file, '/tmp')
    ad = astrodata.open('/tmp/%s' % data_file)

    f = File(data_file)
    df = DiskFile(f, data_file, "")
    df.ad_object = ad
    h = Header(df)
    michelle = Michelle(h, ad)

    assert(michelle.disperser == 'MedN2')
    assert(michelle.filter_name == 'blank')
    assert(michelle.read_mode == 'stare')
    assert(michelle.focal_plane_mask == '2_pixels')
