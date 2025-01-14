# This convenience testing module simply imports all the "liveserver_tests".
# These are tests that can simply be run against an operational server.
# They do no test the local environment where they are being run, they simply
# hit server APIs and test the results against reference values in the tests.


# we want to have pytest assert introspection in the helpers
import pytest
pytest.register_assert_rewrite('fits_storage_tests.liveserver_tests.helpers')

from fits_storage_tests.liveserver_tests.test_jsonfilelist_etc import *
from fits_storage_tests.liveserver_tests.test_spot_check_selections import *
from fits_storage_tests.liveserver_tests.test_cals_gmos import *
from fits_storage_tests.liveserver_tests.test_cals_ghost import *
from fits_storage_tests.liveserver_tests.test_cals_niri import *
from fits_storage_tests.liveserver_tests.test_cals_gnirs import *
from fits_storage_tests.liveserver_tests.test_cals_nifs import *
from fits_storage_tests.liveserver_tests.test_cals_f2 import *
from fits_storage_tests.liveserver_tests.test_cals_igrins2 import *
from fits_storage_tests.liveserver_tests.test_provenance_history import *