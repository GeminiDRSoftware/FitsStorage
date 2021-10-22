import astrodata
from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.file import File
from gemini_obs_db.orm.gpi import Gpi
from gemini_obs_db.orm.header import Header
from tests.file_helper import ensure_file
import gemini_obs_db.db_config as dbc


def test_gpi(monkeypatch):
    monkeypatch.setattr(dbc, "storage_root", "/tmp")

    data_file = 'S20171125S0116.fits'

    ensure_file(data_file, '/tmp')
    ad = astrodata.open('/tmp/%s' % data_file)

    f = File(data_file)
    df = DiskFile(f, data_file, "")
    df.ad_object = ad
    h = Header(df)
    gpi = Gpi(h, ad)

    assert(gpi.filter_name == 'IFSFILT_H_G1213')
    assert(gpi.disperser == 'DISP_WOLLASTON_G6261')
    assert(gpi.focal_plane_mask == 'FPM_H_G6225')
    assert(gpi.pupil_mask == 'APOD_H_G6205')
    assert(gpi.astrometric_standard is False)
    assert(gpi.wollaston is True)
    assert(gpi.prism is False)
