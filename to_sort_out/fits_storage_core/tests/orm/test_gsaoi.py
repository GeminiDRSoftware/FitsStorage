import astrodata
from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.file import File
from gemini_obs_db.orm.gsaoi import Gsaoi
from gemini_obs_db.orm.header import Header
from tests.file_helper import ensure_file
import gemini_obs_db.db_config as dbc


def test_gsaoi(monkeypatch):
    monkeypatch.setattr(dbc, "storage_root", "/tmp")

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
