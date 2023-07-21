# This convenience testing module simply imports all the "code_tests".
# These are tests that can simply be run standalone in a development
# environment. They do no attempt to access a fits_storage web server.
# The may use the helper code to instantiate local test databases and
# use those in the tests, and they do download data from the archive, but
# everything here is standalone and doesn't require a target server.

from tests.code_tests.test_config import *
from tests.code_tests.test_file import *
from tests.code_tests.test_diskfile import *
from tests.code_tests.test_header import *
from tests.code_tests.test_gmos import *
from tests.code_tests.test_gnirs import *
from tests.code_tests.test_gpi import *
from tests.code_tests.test_f2 import *
from tests.code_tests.test_gsaoi import *
from tests.code_tests.test_michelle import *
from tests.code_tests.test_nici import *
from tests.code_tests.test_nifs import *
from tests.code_tests.test_niri import *
from tests.code_tests.test_queueorms import *
from tests.code_tests.test_fileopsqueue import *
from tests.code_tests.test_fileopser import *
from tests.code_tests.test_fitseditor import *
from tests.code_tests.test_ingester import *
from tests.code_tests.test_exporter import *
from tests.code_tests.test_bz2stream import *
from tests.code_tests.test_fileontapehelper import *
from tests.code_tests.test_granthelper import *
from tests.code_tests.test_user_accesscontrol import *
