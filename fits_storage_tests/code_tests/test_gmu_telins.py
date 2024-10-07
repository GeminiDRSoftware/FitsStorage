from fits_storage.gemini_metadata_utils import *


def test_gemini_telescope():
    gnvals = ['Gemini-North', 'gemini-north', 'gemini_north', 'geminiNorth']
    gsvals = ['Gemini-South', 'gemini-south', 'gemini_south', 'Geminisouth']
    badvals = ['Unknown', '', 1, None]

    for i in gnvals:
        assert gemini_telescope(i) == 'Gemini-North'

    for i in gsvals:
        assert gemini_telescope(i) == 'Gemini-South'

    for i in badvals:
        assert gemini_telescope(i) is None


def test_gemini_instrument():
    # Sanity check, we don't test every entry in the dict here
    assert gemini_instrument('NIRI') == 'NIRI'
    assert gemini_instrument('niri') == 'NIRI'
    assert gemini_instrument('GMOS') is None
    assert gemini_instrument('GMOS', gmos=True) == 'GMOS'
    assert gemini_instrument('FUBAR') is None
    assert gemini_instrument('FUBAR', other=True) == 'FUBAR'
    assert gemini_instrument('hokuPAAquIRC') == 'Hokupaa+QUIRC'


def test_gemini_observation_type():
    # Sanity check, we don't test every entry in the dict here
    assert gemini_observation_type('DARK') == 'DARK'
    assert gemini_observation_type('dark') is None
    assert gemini_observation_type('FUBAR') is None
    assert gemini_observation_type('RONCHI') == 'RONCHI'
    assert gemini_observation_type('PINHOLE') == 'PINHOLE'


def test_gemini_observation_class():
    # Sanity check, we don't test every entry in the dict here
    assert gemini_observation_class('science') == 'science'
    assert gemini_observation_class('partnerCal') == 'partnerCal'
    assert gemini_observation_class('partnercal') is None
    assert gemini_observation_class('fubar') is None


def test_gemini_reduction_state():
    # Sanity check, we don't test every entry in the dict here
    assert gemini_reduction_state('RAW') == 'RAW'
    assert gemini_reduction_state('PROCESSED_BPM') == 'PROCESSED_BPM'
    assert gemini_reduction_state('FUBAR') is None
    assert gemini_reduction_state('raw') is None


def test_gemini_caltype():
    # Sanity check, we don't test every entry in the dict here
    assert gemini_caltype('bias') == 'bias'
    assert gemini_caltype('BIAS') is None
    assert gemini_caltype('processed_bpm') == 'processed_bpm'


def test_gmos_gratingname():
    # Sanity check, we don't test every entry in the dict here
    assert gmos_gratingname('MIRROR') == 'MIRROR'
    assert gmos_gratingname('R831') == 'R831'
    assert gmos_gratingname('FUBAR') is None
    assert gmos_gratingname('R123') is None
    assert gmos_gratingname('mirror') is None


def test_gmos_dispersion():
    assert gmos_dispersion('R123') is None
    assert gmos_dispersion('MIRROR') is None
    d = gmos_dispersion('B600')
    e = 0.03/600
    assert d-e < 1E-10


def test_gmos_focal_plane_mask():
    # Sanity check, we don't test every entry in the dict here
    assert gmos_focal_plane_mask('2.0arcsec') == '2.0arcsec'
    assert gmos_focal_plane_mask('IFU-R') == 'IFU-R'
    assert gmos_focal_plane_mask('NS2.0arcsec') == 'NS2.0arcsec'
    assert gmos_focal_plane_mask('fubar') is None
    valid_mos_old = ['GN2012Ax123-12', 'GS2024AQ123-02', 'G2099VC111-22']
    valid_mos_new = ['G2026A0007Q-03', 'G2012B1234F-12']
    invalid_mos = ['GN2020B123Q-12', 'GS2024AQ123-1']

    for i in valid_mos_old:
        assert gmos_focal_plane_mask(i) == i

    for i in valid_mos_new:
        assert gmos_focal_plane_mask(i) == i

    for i in invalid_mos:
        assert gmos_focal_plane_mask(i) is None


def test_gemini_fitsfilename():
    # Sanity check, we don't test every entry in the dict here
    assert gemini_fitsfilename('N20120304S1234.fits') == 'N20120304S1234.fits'
    assert gemini_fitsfilename('N20120304S1234') == 'N20120304S1234.fits'
    assert gemini_fitsfilename('2011apr22_1234') == '2011apr22_1234.fits'
    assert gemini_fitsfilename('fubar.fits') == ''
    assert gemini_fitsfilename('N20120304S1234.fits.bz2') == \
           'N20120304S1234.fits'


def test_gemini_binning():
    # Sanity check, we don't test every entry in the dict here
    assert gemini_binning('1x1') == '1x1'
    assert gemini_binning('2x8') == '2x8'
    assert gemini_binning('1x3') == ''
    assert gemini_binning('') == ''
    assert gemini_binning('fubar') == ''


def test_percentilestring():
    # Sanity check, we don't test every entry in the dict here
    assert percentilestring(20, 'IQ') == 'IQ20'
    assert percentilestring(100, 'IQ') == 'IQAny'
    assert percentilestring(None, 'IQ') == 'Undefined'


def test_site_monitor():
    assert site_monitor('GS_ALLSKYCAMERA') is True
    assert site_monitor('GMOS-S') is False
    assert site_monitor('fubar') is False
