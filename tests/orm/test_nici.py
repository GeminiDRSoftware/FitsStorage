import astrodata
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.file import File
from fits_storage.orm.header import Header
from fits_storage.orm.nici import Nici
from tests.file_helper import ensure_file
import fits_storage.fits_storage_config as fsc


def test_nici(monkeypatch):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    data_file = 'S20130123S0131.fits'

    ensure_file(data_file, '/tmp')
    ad = astrodata.open('/tmp/%s' % data_file)

    f = File(data_file)
    df = DiskFile(f, data_file, "")
    df.ad_object = ad
    h = Header(df)
    nici = Nici(h, ad)

    assert(nici.filter_name == 'CH4-H4%L_G0740+CH4-H4%S_G0743')
    assert(nici.focal_plane_mask is None)
    assert(nici.disperser is None)


def test_nici_cal(monkeypatch):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    data_file = 'S20130124S0036.fits'

    ensure_file(data_file, '/tmp')
    ad = astrodata.open('/tmp/%s' % data_file)

    f = File(data_file)
    df = DiskFile(f, data_file, "")
    df.ad_object = ad
    h = Header(df)
    nici = Nici(h, ad)

    assert(nici.filter_name == 'Kprime_G0706+CH4-H4%S_G0743')
    assert(nici.focal_plane_mask is None)
    assert(nici.disperser is None)
