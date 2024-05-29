from fits_storage_tests.liveserver_tests.helpers import calhelper, getserver


# This dict of dicts defines the expected calibration associations.
cals_2022_niri = {
    # NIRI Imaging L'
    'GN-2022A-Q-131-6-001': {'bias': None,
                             'dark': 'GN-2022A-Q-131-7-001',
                             'flat': None,  # L-prime
                             'arc': None,
                             'processed_bpm': 'GN-2012B-DD-5-15-099-BPM',
                             },
    # NIRI Imaging K
    'GN-2022A-Q-106-53-001': {'bias': None,
                              'dark': 'GN-2022A-Q-106-54-001',
                              'flat': 'GN-2022A-Q-106-50-011',
                              'arc': None,
                              'lampoff_flat': 'GN-2022A-Q-106-50-001',
                              'processed_bpm': 'GN-2012B-DD-5-15-099-BPM',
                              },
    # NIRI Imaging J
    'GN-2021A-Q-319-35-001': {'bias': None,
                              'dark': 'GN-2021A-Q-319-81-041',
                              'flat': 'GN-2021A-Q-319-81-011',
                              'arc': None,
                              'lampoff_flat': 'GN-2021A-Q-319-81-001',
                              'processed_bpm': 'GN-2012B-DD-5-15-099-BPM',
                              },
}

def test_niricals():
    calhelper(getserver(), cals_2022_niri)