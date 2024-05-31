# This convenience testing module simply imports all the "code_tests".
# These are tests that can simply be run standalone in a development
# environment. They do no attempt to access a fits_storage web server.
# The may use the helper code to instantiate local test databases and
# use those in the tests, and they do download data from the archive, but
# everything here is standalone and doesn't require a target server.

from fits_storage_tests.code_tests.test_config import *
from fits_storage_tests.code_tests.test_file import *
from fits_storage_tests.code_tests.test_diskfile import *
from fits_storage_tests.code_tests.test_header import *
from fits_storage_tests.code_tests.test_gmos import *
from fits_storage_tests.code_tests.test_gnirs import *
from fits_storage_tests.code_tests.test_gpi import *
from fits_storage_tests.code_tests.test_f2 import *
from fits_storage_tests.code_tests.test_gsaoi import *
from fits_storage_tests.code_tests.test_michelle import *
from fits_storage_tests.code_tests.test_nici import *
from fits_storage_tests.code_tests.test_nifs import *
from fits_storage_tests.code_tests.test_niri import *
from fits_storage_tests.code_tests.test_queueorms import *
from fits_storage_tests.code_tests.test_fileopsqueue import *
from fits_storage_tests.code_tests.test_fileopser import *
from fits_storage_tests.code_tests.test_fitseditor import *
from fits_storage_tests.code_tests.test_ingester import *
from fits_storage_tests.code_tests.test_exporter import *
from fits_storage_tests.code_tests.test_bz2stream import *
from fits_storage_tests.code_tests.test_granthelper import *
from fits_storage_tests.code_tests.test_user_accesscontrol import *
from fits_storage_tests.code_tests.test_provenance_history import *
from fits_storage_tests.code_tests.test_gmu_telins import *
from fits_storage_tests.code_tests.test_gmu_progidetc import *
from fits_storage_tests.code_tests.test_odb_interface import *
