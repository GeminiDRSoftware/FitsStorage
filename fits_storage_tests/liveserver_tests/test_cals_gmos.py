from fits_storage_tests.liveserver_tests.helpers import calhelper

# The server to test against
server = 'https://archive.gemini.edu:/jsoncalmgr'

# This dict of dicts defines the expected calibration associations.
cals = {
    # GMOS imaging
    'GN-2019B-Q-122-23-001': {'bias': 'GN-CAL20200101-24-031',
                            'flat': 'GN-CAL20191231-30-010',
                            'photometric_standard': 'GN-CAL20200101-26-004',
                            'dark': None,
                            'arc': None,
                            # TODO - add processed BPM
                            },
    # GMOS longslit
    'GN-2019B-FT-214-24-003': {'bias': 'GN-CAL20200101-24-021',
                            'flat': 'GN-2019B-FT-214-24-002',
                            'arc': 'GN-2019B-FT-214-25-001',
                            'dark': None,
                            # TODO - add processed BPM
                            }
}

def test_gmoscals():
    calhelper(server, cals)