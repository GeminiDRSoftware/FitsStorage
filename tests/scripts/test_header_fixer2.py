import os
import tempfile
from os.path import basename, dirname

import fits_storage
from fits_storage.scripts.header_fixer2 import fix_igrins, fix_and_copy


class MockPHU(object):
    def __init__(self, dict={}):
        self.header = dict


def mock_fits(dict={}):
    fits = list()
    phu = MockPHU(dict)
    fits.append(phu)
    return fits


def test_no_instrument():
    assert(not fix_igrins(mock_fits()))


def test_not_igrins():
    assert(not fix_igrins(mock_fits({"INSTRUME": "GMOS-N"})))


def test_dont_fix_existing_gemprgid():
    fits = mock_fits({"INSTRUME": "IGRINS", "GEMPRGID": "prog", "GEMPRID": "pro", "OBSID": "obsid", "DATALAB": "datalab"})
    assert(not fix_igrins(fits))  # nothing to fix
    assert(fits[0].header['GEMPRGID'] == "prog")


def test_fix_gemprid():
    fits = mock_fits({"INSTRUME": "IGRINS", "GEMPRID": "prog"})
    assert(fix_igrins(fits))
    assert(fits[0].header['GEMPRGID'] == "prog")


def test_fix_missing_obsid():
    fits = mock_fits({"INSTRUME": "IGRINS", "GEMPRID": "prog"})
    assert(fix_igrins(fits))
    assert (fits[0].header['OBSID'] == "prog-0")


def test_dont_fix_existing_obsid():
    fits = mock_fits({"INSTRUME": "IGRINS", "GEMPRID": "prog", "OBSID": "obsid"})
    assert(fix_igrins(fits))
    assert (fits[0].header['OBSID'] == "obsid")


def test_fix_numeric_obsid():
    fits = mock_fits({"INSTRUME": "IGRINS", "GEMPRID": "prog", "OBSID": 2})
    assert(fix_igrins(fits))
    assert (fits[0].header['OBSID'] == "prog-2")


def test_dont_fix_existing_datalab():
    fits = mock_fits({"INSTRUME": "IGRINS", "GEMPRGID": "prog", "OBSID": "obsid", "DATALAB": "datalab"})
    assert(not fix_igrins(fits))
    assert (fits[0].header['DATALAB'] == "datalab")


def test_fix_datalab():
    fits = mock_fits({"INSTRUME": "IGRINS", "GEMPRGID": "prog", "OBSID": "obsid"})
    assert(fix_igrins(fits))
    assert (fits[0].header['DATALAB'] == "obsid-0")


def test_dont_fix_exiting_release():
    fits = mock_fits({"INSTRUME": "IGRINS", "GEMPRGID": "prog", "DATE-OBS": "2018-01-01", "RELEASE": "2020-01-01"})
    assert(fix_igrins(fits))
    assert (fits[0].header['RELEASE'] == "2020-01-01")


def test_dont_fix_exiting_release():
    fits = mock_fits({"INSTRUME": "IGRINS", "GEMPRGID": "prog", "DATE-OBS": "2018-01-01"})
    assert(fix_igrins(fits))
    assert (fits[0].header['RELEASE'] == "2019-01-01")


def test_email_on_error(monkeypatch):
    global saw_error_email
    saw_error_email = False

    def mocksendmail(subject, mailfrom, mailto, messages):
        global saw_error_email
        saw_error_email = True

    monkeypatch.setattr('fits_storage.scripts.header_fixer2.sendmail', mocksendmail)
    with tempfile.NamedTemporaryFile() as fp:
        fp.write(b'Hello world!')
        fp.close()
        fullname = fp.name
        filename = basename(fullname)
        src = dirname(fullname)
        dst = tempfile.mkdtemp(suffix="_FitsStorage_test_header_fixer2")
        fix_and_copy(src, dst, filename, compress=True,
                     mailfrom="fitsdata@gemini.edu", mailto="ooberdorf@gemini.edu")
        os.rmdir(dst)
        assert saw_error_email
