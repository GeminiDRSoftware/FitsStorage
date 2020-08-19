import astrodata
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.file import File
from fits_storage.orm.fulltextheader import FullTextHeader
import fits_storage.fits_storage_config as fsc
from tests.file_helper import ensure_file


def test_fulltextheader(monkeypatch):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")
    data_file = 'S20181219S0333.fits'

    ensure_file(data_file, '/tmp')
    ad = astrodata.open('/tmp/%s' % data_file)

    f = File(data_file)
    df = DiskFile(f, data_file, "")
    df.ad_object = ad
    fth = FullTextHeader(df)
    assert('PHU' in fth.fulltext)