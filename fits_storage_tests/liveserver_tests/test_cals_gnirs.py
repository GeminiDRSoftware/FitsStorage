from fits_storage_tests.liveserver_tests.helpers import calhelper, getserver


# This dict of dicts defines the expected calibration associations.
cals_2022_gnirs = {
    # GNIRS Imaging JPHOT
    'GN-2022A-Q-326-16-001': {'bias': None,
                              'dark': 'GN-2022A-Q-326-18-001',
                              'flat': 'GN-2022A-Q-326-16-010',
                              'arc': None,
                              'lampoff_flat': 'GN-2022A-Q-326-16-018',
                              'processed_bpm': 'GN-CAL20121205-10-001-BPM'
                              },
    # GNIRS LS 111l/mm J
    'GN-2021B-Q-212-180-001': {'bias': None,
                               'dark': None,
                               'flat': 'GN-2021B-Q-212-180-009',
                               'arc': 'GN-2021B-Q-212-180-014',
                               'telluric_standard': 'GN-2021B-Q-212-266-012',
                               'processed_bpm': 'GN-CAL20121205-10-001-BPM',
                               },
    # GNIRS XD 32l/mm H
    'GN-2021B-Q-308-295-001': {'bias': None,
                               'dark': None,
                               'flat': 'GN-2021B-Q-308-295-013',
                               'arc': 'GN-2021B-Q-308-295-029',
                               'pinhole': 'GN-2021B-Q-308-238-001',
                               'processed_bpm': 'GN-CAL20121205-10-001-BPM',
                               },
    # GNIRS XD 111l/mm K
    'GN-2021B-Q-305-18-001': {'bias': None,
                              'dark': None,
                              'flat': 'GN-2021B-Q-305-18-005',
                              'arc': 'GN-2021B-Q-305-27-001',
                              'pinhole': 'GN-2021B-Q-305-28-001',
                              'processed_bpm': 'GN-CAL20121205-10-001-BPM',
                              },
}


def test_gnirscals():
    calhelper(getserver(), cals_2022_gnirs)