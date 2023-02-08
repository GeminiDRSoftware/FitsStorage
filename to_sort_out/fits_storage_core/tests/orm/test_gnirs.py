import astrodata
from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.file import File
from gemini_obs_db.orm.gnirs import Gnirs
from gemini_obs_db.orm.header import Header
from tests.file_helper import ensure_file
import gemini_obs_db.db_config as dbc


def test_gnirs(monkeypatch):
    monkeypatch.setattr(dbc, "storage_root", "/tmp")

    data_file = 'N20180524S0117.fits'

    ensure_file(data_file, '/tmp')
    ad = astrodata.open('/tmp/%s' % data_file)

    f = File(data_file)
    df = DiskFile(f, data_file, "")
    df.ad_object = ad
    h = Header(df)
    gnirs = Gnirs(h, ad)

    assert(gnirs.disperser == 'MIRROR')
    assert(gnirs.filter_name == 'Open&H2_G0522')
    assert(gnirs.read_mode == 'Very Bright Objects')
    assert(gnirs.focal_plane_mask == 'Acq_G5530&Acq_G5531')
    assert(gnirs.camera == 'LongBlue_G5542')
    assert(gnirs.well_depth_setting == 'Shallow')
