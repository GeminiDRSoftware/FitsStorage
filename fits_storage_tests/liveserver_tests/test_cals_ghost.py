import pytest
pytest.register_assert_rewrite('fits_storage_tests.liveserver_tests.helpers')

from fits_storage_tests.liveserver_tests.helpers import calhelper, getserver

# This dict of dicts defines the expected calibration associations.
cals_2024_ghost = {
    'GS-CAL20240320-1-001': {'bias': 'GS-CAL20240323-15-001',
                             'flat': 'GS-CAL20240323-1-001',
                             'arc': 'GS-CAL20240316-3-001',
                             },
}


def test_ghostcals():
    calhelper(getserver(), cals_2024_ghost)
