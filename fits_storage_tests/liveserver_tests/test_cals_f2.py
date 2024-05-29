from fits_storage_tests.liveserver_tests.helpers import calhelper, getserver


# This dict of dicts defines the expected calibration associations.
cals_2022_f2 = {
    # J imaging
    'GS-2021B-DD-105-9-001': {'bias': None,
                              'dark': 'GS-CAL20220101-2-099',
                              'flat': 'GS-2021A-Q-120-79-012',
                              'arc': None,
                              },
    # K-blue imaging
    'GS-2021B-LP-103-138-001': {'bias': None,
                                'dark': 'GS-CAL20211226-3-070',
                                'flat': None,  # WTF...
                                'arc': None,
                                'photometric_standard': 'GS-2021B-LP-103-137-001',
                                },
    # HK Spectroscopy
    'GS-2021B-Q-233-59-001': {'bias': None,
                              'dark': 'GS-CAL20211226-3-119',
                              'flat': 'GS-2021B-Q-233-61-005',
                              'arc': 'GS-2021B-Q-233-59-012',
                              'telluric_standard': 'GS-2021B-Q-233-62-001',
                              },
    # JH Spectroscopy
    'GS-2022A-Q-235-26-001': {'bias': None,
                              'dark': 'GS-CAL20220205-1-162',
                              'flat': 'GS-2022A-Q-235-26-005',
                              'arc': 'GS-2022A-Q-235-26-006',
                              'telluric_standard': 'GS-2022A-Q-235-28-001',
                              },
    }


def test_f2cals():
    calhelper(getserver(), cals_2022_f2)