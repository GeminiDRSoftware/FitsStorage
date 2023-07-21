from fits_storage_tests.liveserver_tests.helpers import calhelper

# The server to test against
server = 'https://archive.gemini.edu:/jsoncalmgr'

# This dict of dicts defines the expected calibration associations.
cals_2022_gmos = {
    # GMOS imaging
    'GN-2021B-Q-117-83-001': {'bias': 'GN-CAL20220108-4-031',
                             'flat': 'GN-CAL20220122-7-004',
                             'photometric_standard': 'GN-CAL20220108-2-002',
                             'dark': None,
                             'arc': None,
                             # TODO - add processed BPM
                             },
    # GMOS longslit
    'GN-2022A-Q-303-9-001': {'bias': 'GN-CAL20220108-4-086',
                             'flat': 'GN-2022A-Q-303-9-003',
                             'arc': 'GN-2022A-Q-303-10-001',
                             'dark': None,
                             'specphot': 'GN-2022A-Q-303-10-001',
                             # TODO - add processed BPM
                             },
    # GMOS MOS
    'GN-2021B-Q-107-91-001': {'bias': 'GN-CAL20220108-4-051',
                              'flat': 'GN-2021B-Q-107-91-004',
                              'arc': 'GN-2021B-Q-107-50-001',
                              'dark': None,
                              'spectwilight': 'GN-2021B-Q-107-89-001',
                              'mask': 'GN2021BQ107-01',
                              'slitillum': 'GN-2021B-Q-107-89-001',
                              # TODO - add processed BPM
                              },
    # GMOS IFU
    'GN-2021B-FT-212-12-002': {'bias': 'GN-CAL20220123-2-001',
                               'flat': 'GN-2021B-FT-212-12-001',
                               'arc': 'GN-2021B-FT-212-16-001',
                               'dark': None,
                               'spectwilight': 'GN-2021B-FT-212-4-001',
                               'specphot': 'GN-2021B-FT-212-7-001',
                               'slitillum': 'GN-2021B-FT-212-4-001',
                               # TODO - add processed BPM
                               },
}

def test_gmoscals():
    calhelper(server, cals_2022_gmos)