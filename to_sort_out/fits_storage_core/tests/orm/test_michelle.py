import astrodata
from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.file import File
from gemini_obs_db.orm.header import Header
from gemini_obs_db.orm.michelle import Michelle
from tests.file_helper import ensure_file
import gemini_obs_db.db_config as dbc

def test_michelle(monkeypatch):
    monkeypatch.setattr(dbc, "storage_root", "/tmp")

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
