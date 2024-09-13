import pytest
pytest.register_assert_rewrite('fits_storage_tests.liveserver_tests.helpers')

from fits_storage_tests.liveserver_tests.helpers import getserver, calhelper, \
    associatedcalhelper, calibrationshelper


# This dict of dicts defines the expected calibration associations.
cals_2022_nifs = {
    'GN-2022A-Q-315-12-001': {'bias': None,
                              'dark': None,
                              'flat': 'GN-2022A-Q-315-15-001',
                              'arc': 'GN-2022A-Q-315-12-010',
                              'ronchi_mask': 'GN-2022A-Q-315-15-013',
                              'telluric_standard': 'GN-2022A-Q-315-10-006',
                              },
}


def test_nifscals():
    calhelper(getserver(), cals_2022_nifs)


def test_nifscalibrations():
    calibrationshelper(getserver(), cals_2022_nifs)


def test_associated_nifscals():
    associatedcalhelper(getserver(), cals_2022_nifs)
