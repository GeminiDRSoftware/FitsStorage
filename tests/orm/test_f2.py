import astrodata
from fits_storage.orm.f2 import F2
import fits_storage.fits_storage_config as fsc
from tests.file_helper import ensure_file


class MockHeader:
    def __init__(self, id):
        self.id = id


def test_f2(monkeypatch):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    h = MockHeader(123)
    data_file = 'S20181219S0333.fits'

    ensure_file(data_file, '/tmp')
    ad = astrodata.open('/tmp/%s' % data_file)

    f2 = F2(h, ad)

    assert (f2.disperser == 'JH_G5801')
    assert (f2.filter_name == 'Open&JH_G0809')
    assert (f2.lyot_stop == 'f/16_G5830')
    assert (f2.read_mode == '1')
    assert (f2.focal_plane_mask == '4pix-slit')
