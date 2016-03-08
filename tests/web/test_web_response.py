import pytest
import requests

SERVER = 'http://rcardene-lv1'

GET = 'GET'
POST = 'POST'
OK = 200
BAD_REQUEST = 400
FORBIDDEN = 403
NOT_FOUND = 404
NOT_ALLOWED = 405
NOT_ACCEPTABLE = 406
INTERNAL_ERROR = 500

no_auth_no_data = (
    ('debug', GET, OK),
#    ('content', GET, OK),  # Veeeery slow
#    ('stats', GET, OK),    # Veeery slow
    ('qareport', GET, NOT_ALLOWED),
    ('qareport', POST, BAD_REQUEST),
    ('usagereport', GET, FORBIDDEN),
    ('usagestats', GET, FORBIDDEN),
#    ('xmltape', GET, OK),  # Veeeeeeeery slow
    ('taperead', GET, OK),
    ('notification', GET, FORBIDDEN),
    ('import_odb_notifications', GET, NOT_ALLOWED),
    ('import_odb_notifications', POST, BAD_REQUEST),
    ('request_password_reset', GET, OK),
    ('logout', GET, OK),
    ('user_list', GET, OK),
    ('update_headers', GET, NOT_ALLOWED),
    ('update_headers', POST, FORBIDDEN),
    ('ingest_files', GET, NOT_ALLOWED),
    ('ingest_files', POST, OK),
#    ('curation', GET, OK),  # Ssssssslow
    ('staff_access', GET, OK),
    ('nameresolver/bar', GET, NOT_FOUND),
    ('nameresolver/foo/bar', GET, NOT_ACCEPTABLE),
    ('fileontape', GET, NOT_FOUND),
    ('fileontape/bar', GET, OK),
    ('file', GET, NOT_FOUND),
    ('file/foo.bar', GET, NOT_FOUND),
    ('download', GET, OK),
    ('download', POST, OK),
    ('qametrics', GET, OK),
    ('qaforgui', GET, NOT_FOUND),
    ('qaforgui/20150101', GET, OK),
    ('usagedetails', GET, NOT_FOUND),
    ('usagedetails/12345', GET, FORBIDDEN),
    ('usagedetails/12345', GET, FORBIDDEN),
    ('downloadlog', GET, FORBIDDEN),
    ('tape', GET, OK),
    ('tape/foobar', GET, OK),
    ('tapewrite', GET, OK),
    ('tapewrite/foobar', GET, OK),
    ('tapefile/a', GET, NOT_FOUND),
    ('tapefile/10', GET, OK),
    ('tapefile/10', GET, OK),
    ('request_account', GET, OK),
    ('password_reset', GET, NOT_FOUND),
    ('password_reset/1/foobarbaz', GET, OK),
    ('login', GET, OK),
    ('whoami', GET, OK),
    ('change_password', GET, OK),
    ('my_programs', GET, OK),
    ('preview/foobar', GET, NOT_FOUND),
    ('queuestatus', GET, FORBIDDEN),
    ('queuestatus/json', GET, FORBIDDEN),
    ('queuestatus/iq/10', GET, FORBIDDEN),
    ('miscfiles', GET, OK),
    ('miscfiles/10', GET, NOT_FOUND),
    ('miscfiles/validate_add', GET, NOT_ALLOWED),
    ('miscfiles/validate_add', POST, BAD_REQUEST),
    ('miscfiles/validate_add', (POST, '{"release":"2017-12-01"}'), OK),
    ('standardobs/10', GET, OK),
    ('upload_file/fn', GET, NOT_ALLOWED),
    ('upload_file/fn', POST, BAD_REQUEST),
    ('upload_file/fn', (POST, '{}'), FORBIDDEN),
    ('upload_processed_cal/pcal', GET, NOT_ALLOWED),
    ('upload_processed_cal/pcal', POST, FORBIDDEN),
    ('fitsverify/10', GET, OK),
    ('fitsverify/foo', GET, NOT_FOUND),
    ('mdreport/10', GET, OK),
    ('mdreport/bar', GET, NOT_FOUND),
    ('fullheader/10', GET, OK),
    ('fullheader/baz', GET, NOT_FOUND),
    ('calibrations/20130101', GET, OK),
    ('xmlfilelist/20130101', GET, OK),
    ('jsonfilelist/20130101', GET, OK),
    ('jsonfilenames/20130101', GET, OK),
    ('jsonsummary/20130101', GET, OK),
    ('jsonqastate/20130101', GET, OK),
    ('calmgr/N20130101S0001.fits', GET, OK),
    ('gmoscal/20130101', GET, OK),
    ('programsobserved/20130101', GET, OK),
    ('obslogs/20130101', GET, OK),
    ('associated_obslogs/20130101', GET, OK),
    ('gmoscaljson/20130101', GET, OK),
    )

def perform_test(url, method=GET, *args, **kw):
    if isinstance(method, tuple):
        method, data = method
        kw['data'] = data
    else:
        data = ''
    req = requests.post if method == POST else requests.get

    r = req(url, *args, **kw)

    return r.status_code

@pytest.mark.parametrize("input,method,expected", no_auth_no_data)
def test_simple_get_query(input, method, expected):
    url = '/'.join([SERVER,input])
    assert perform_test(url, method=method) == expected
