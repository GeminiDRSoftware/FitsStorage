import pytest
pytest.register_assert_rewrite('fits_storage_tests.liveserver_tests.helpers')

from fits_storage_tests.liveserver_tests.helpers import getserver, calhelper, \
    associatedcalhelper


# This dict of dicts defines the expected calibration associations.
cals_2024_igrins2 = {
    'GN-2024B-SV-111-26-001': {'bias': None,
                              'dark': None,
                              'flat': 'GN-CAL20240721-12-020',
                              'arc': 'GN-2024A-CAL-141-11-001',
                              },
}


def test_igrins2cals():
    calhelper(getserver(), cals_2024_igrins2)


def test_associated_igrins2cals():
    associatedcalhelper(getserver(), cals_2024_igrins2)
