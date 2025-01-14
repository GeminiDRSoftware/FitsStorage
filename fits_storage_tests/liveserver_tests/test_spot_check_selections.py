import pytest

from fits_storage_tests.liveserver_tests.helpers import \
    selection_spotcheck_helper

pytest.register_assert_rewrite('fits_storage_tests.liveserver_tests.helpers')

# List of (selection, number, filename) tuples.
gmos_spec_spot_checks = [
    ('defaults/date=20200101/disperser=R400/GMOS-S/science', 12,
     'S20200101S0156.fits'),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/1x1', 12,
     'S20200101S0156.fits'),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/1x2', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/1x4', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/2x1', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/2x2', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/2x4', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/4x1', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/4x2', 0, None),
    ('defaults/date=20200101/disperser=R400/GMOS-S/science/4x4', 0, None),
    ('/defaults/date=20200101/disperser=R400/GMOS-S/science/imaging', 0, None),
    ('/defaults/date=20200101/disperser=R400/GMOS-S/science/spectroscopy', 12,
     'S20200101S0156.fits'),
    ('/defaults/date=20200101/disperser=R400/GMOS-S/science/imaging', 0, None),
    ('/defaults/date=20200101/disperser=R400/GMOS-S/science/spectroscopy/LS',
     0, None),
    ('/defaults/date=20200101/disperser=R400/GMOS-S/science/spectroscopy/MOS',
     0, None),
    ('/defaults/date=20200101/disperser=R400/GMOS-S/science/spectroscopy/IFS',
     12, 'S20200101S0156.fits'),
]


def test_gmos_spec_spotchecks():
    selection_spotcheck_helper(gmos_spec_spot_checks)
