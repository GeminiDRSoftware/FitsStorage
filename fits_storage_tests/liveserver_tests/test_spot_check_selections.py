import pytest

from fits_storage_tests.liveserver_tests.helpers import \
    selection_spotcheck_helper

pytest.register_assert_rewrite('fits_storage_tests.liveserver_tests.helpers')

# List of (selection, number, filename) tuples.
gmos_spec_spotchecks = [
    ('defaults/date=20200101/disperser=R400/GMOS-S/science', 12, 'S20200101S0156.fits'),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/1x1', 12, 'S20200101S0156.fits'),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/1x2', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/1x4', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/2x1', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/2x2', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/2x4', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/4x1', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/4x2', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/4x4', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/imaging', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/spectroscopy', 12, 'S20200101S0156.fits'),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/imaging', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/spectroscopy/LS', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/spectroscopy/MOS', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/spectroscopy/IFS', 12, 'S20200101S0156.fits'),
]

progidetc_spotchecks = [
    ('defaults/GS-2019B-Q-229', 542, 'S20200101S0043.fits'),
    ('defaults/GS-2019B-Q-229-129', 12, 'S20200101S0043.fits'),
    ('defaults/GS-2019B-Q-229-129-001', 1, 'S20200101S0043.fits'),
    ('defaults/GS-2019B-FT-209', 62, 'S20200101S0002.fits'),
    ('defaults/GS-2019B-FT-209-12', 22, 'S20191221S0056.fits'),
    ('defaults/GS-2019B-FT-209-12-004', 1, 'S20191221S0059.fits'),
]

datetime_spotchecks = [
    ('defaults/date=20200101', 542, 'S20200101S0002.fits'),
    ('defaults/date=20200110', 810, 'S20200110S0054.fits'),
    ('defaults/night=20200110', 657, 'S20200110S0019.fits'),

]
inst_spotchecks = [
    ('defaults/date=20200101/F2', 146, 'S20200101S0043.fits'),
    ('defaults/date=20200101/GMOS-N', 193, 'N20200101S0039.fits'),
    ('defaults/date=20200101/GMOS-S', 99, 'S20200101S0002.fits'),
    ('defaults/date=20200101/GMOS', 292, 'S20200101S0002.fits'),
    ('defaults/date=20200101/GNIRS', 42, 'N20200101S0002.fits'),
    ('defaults/date=20200101/NIFS', 62, 'N20200101S0007.fits'),
    ('defaults/date=20200127/NIRI', 532, 'N20200127S0023.fits'),
    ('defaults/date=20200302/GSAOI', 135, 'S20200303S0025.fits'),
    ('defaults/date=20230214/GHOST', 26, 'S20230215S0011.fits'),
    ('defaults/date=20250109/IGRINS-2', 41, 'N20250109S0281.fits'),
    ('defaults/date=20200131/IGRINS', 99, 'SDCS_20200130_0013.fits'),
    ('defaults/date=20200215/ALOPEKE', 340, 'N20200215A0169r.fits'),
    ('defaults/date=20200108/ZORRO', 274, 'S20200108Z0001b.fits'),
    ('defaults/date=20241227/MAROON-X', 29, 'N20241227M0100.fits'),
    ('defaults/date=20221006/GRACES', 108, 'N20221006G0005.fits')
]

def test_progidetc_spotchecks():
    selection_spotcheck_helper(progidetc_spotchecks)

def test_datetime_spotchecks():
    selection_spotcheck_helper(datetime_spotchecks)

def test_inst_spotchecks():
    selection_spotcheck_helper(inst_spotchecks)

def test_gmos_spec_spotchecks():
    selection_spotcheck_helper(gmos_spec_spotchecks)

