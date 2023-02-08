import astrodata
from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.file import File
from gemini_obs_db.orm.header import Header
from gemini_obs_db.orm.niri import Niri
from tests.file_helper import ensure_file
import gemini_obs_db.db_config as dbc


def test_niri(monkeypatch):
    monkeypatch.setattr(dbc, "storage_root", "/tmp")

    data_file = 'N20180329S0134.fits'

    ensure_file(data_file, '/tmp')
    ad = astrodata.open('/tmp/%s' % data_file)

    f = File(data_file)
    df = DiskFile(f, data_file, "")
    df.ad_object = ad
    h = Header(df)
    niri = Niri(h, ad)

    assert(niri.focal_plane_mask == 'f6-cam_G5208')
    assert(niri.disperser == 'MIRROR')
    assert(niri.read_mode == 'Medium Background')
    assert(niri.filter_name == 'J_G0202')
    assert(niri.well_depth_setting == 'Shallow')
    assert(niri.data_section == 'Section(x1=0, x2=1024, y1=0, y2=1024)')
    assert(niri.camera == 'f6')
    assert(niri.focal_plane_mask == 'f6-cam_G5208')
