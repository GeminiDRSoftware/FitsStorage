import datetime
from ast import literal_eval

from fits_storage.core.orm.header import Header

from .helpers import make_diskfile


def test_header(tmp_path):
    diskfile = make_diskfile('N20200127S0023.fits.bz2', tmp_path)
    header = Header(diskfile)

    assert header.program_id == 'GN-2019B-FT-111'
    assert header.engineering is False
    assert header.science_verification is False
    assert header.calibration_program is False
    assert header.procmode is None
    assert header.observation_id == 'GN-2019B-FT-111-31'
    assert header.data_label == 'GN-2019B-FT-111-31-001'
    assert header.telescope == 'Gemini-North'
    assert header.instrument == 'NIRI'
    assert header.ut_datetime == \
           datetime.datetime(2020, 1, 27, 6, 11, 21, 900000)
    assert header.ut_datetime_secs == 633420681
    assert header.local_time == datetime.time(20, 11, 21, 900000)
    assert header.observation_type == 'OBJECT'
    assert header.observation_class == 'science'
    assert header.object == 'WD 0145+234'
    assert 26.9777778355665 < header.ra < 26.9777778355667
    assert 23.6612859746210 < header.dec < 23.6612859746212
    assert 283.411870833332 < header.azimuth < 283.411870833334
    assert 56.3401763888888 < header.elevation < 56.3401763888890
    assert 179.864887198980 < header.cass_rotator_pa < 179.864887198982
    assert 1.200 < header.airmass < 1.202
    assert header.filter_name == 'J'
    assert 10.00 < header.exposure_time < 10.01
    assert header.disperser == 'MIRROR'
    assert header.camera == 'f6'
    assert header.central_wavelength is None
    assert header.wavelength_band == 'J'
    assert header.focal_plane_mask == 'f6-cam'
    assert header.pupil_mask == 'MIRROR'
    assert header.detector_binning == '1x1'
    assert header.detector_roi_setting == 'Full Frame'
    assert header.detector_gain_setting is None
    assert header.detector_readspeed_setting is None
    assert header.detector_welldepth_setting == 'Shallow'
    assert header.detector_readmode_setting == 'Medium_Background'
    assert header.coadds == 10
    assert header.spectroscopy is False
    assert header.mode == 'imaging'
    assert header.adaptive_optics is False
    assert header.laser_guide_star is False
    assert header.wavefront_sensor == 'PWFS2'
    assert header.gcal_lamp is None
    assert header.raw_iq == 70
    assert header.raw_cc == 50
    assert header.raw_wv == 20
    assert header.raw_bg == 20
    assert header.requested_iq == 85
    assert header.requested_cc == 70
    assert header.requested_wv == 100
    assert header.requested_bg == 100
    assert header.qa_state == 'Usable'
    assert header.release == datetime.date(2020, 7, 27)
    assert header.reduction == 'RAW'
    assert header.site_monitoring is False
    assert literal_eval(header.types) == {'NIRI', 'RAW', 'NORTH', 'IMAGE',
                                          'SIDEREAL', 'GEMINI', 'UNPREPARED'}
    assert header.phot_standard is None
    assert header.proprietary_coordinates is False
    assert header.pre_image is False
