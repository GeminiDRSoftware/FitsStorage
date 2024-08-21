# This convenience testing module simply imports all the "testserver_tests".
# These tests test against a running web server. You should stand up a
# fits storage server with a web server running and an empty database
# then run these tests pointing at it. The server needs to have
# service_queue tasks running for ingest and fileops.
# The server name defaults to http://localhost:8000 but can set by setting
# the FITS_STORATE_TEST_TESTSERVER environment variable.

from fits_storage_tests.testserver_tests.\
    test_upload_ingest_against_web_server import *
from fits_storage_tests.testserver_tests.\
    test_jsonqareport_against_web_server import *
from fits_storage_tests.testserver_tests.test_ingest_programs import *
from fits_storage_tests.testserver_tests.test_jsonfilelist_etc import *
