import astrodata
from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.file import File
from gemini_obs_db.orm.gmos import Gmos
from gemini_obs_db.orm.header import Header
from tests.file_helper import ensure_file
import gemini_obs_db.db_config as dbc


def test_gmos(monkeypatch):
    monkeypatch.setattr(dbc, "storage_root", "/tmp")

    data_file = 'N20191002S0080.fits'

    ensure_file(data_file, '/tmp')
    ad = astrodata.open('/tmp/%s' % data_file)

    f = File(data_file)
    df = DiskFile(f, data_file, "")
    df.ad_object = ad
    h = Header(df)
    gmos = Gmos(h, ad)

    assert(gmos.disperser == 'R400+_G5305')
    assert(gmos.filter_name == 'OG515_G0306&open2-8')
    assert(gmos.detector_x_bin == 2)
    assert(gmos.detector_y_bin == 2)
    assert(gmos.gain_setting == 'low')
    assert(gmos.read_speed_setting == 'slow')
    assert(gmos.focal_plane_mask == '5.0arcsec')
    assert(gmos.nodandshuffle is False)
    assert(gmos.nod_count is None)
    assert(gmos.nod_pixels is None)
    assert(gmos.grating_order is None)
    assert(gmos.prepared is False)
    assert(gmos.overscan_trimmed is False)
    assert(gmos.overscan_subtracted is False)

