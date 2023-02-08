import os
import tempfile
from os.path import basename, dirname
import fits_storage
from fits_storage.scripts.header_fixer2 import fix_igrins, fix_and_copy, fix_zorro_or_alopeke


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
    fix_igrins(fits)  # nothing to fix
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
    fits = mock_fits({"INSTRUME": "IGRINS", "GEMPRID": "KITTENS", "OBSID": "PUPPIES-0"})
    assert(fix_igrins(fits))
    assert (fits[0].header['OBSID'] == "PUPPIES-0")


def test_fix_numeric_obsid():
    fits = mock_fits({"INSTRUME": "IGRINS", "GEMPRID": "prog", "OBSID": 2})
    assert(fix_igrins(fits))
    assert (fits[0].header['OBSID'] == "prog-2")


def test_dont_fix_existing_datalab():
    fits = mock_fits({"INSTRUME": "IGRINS", "GEMPRGID": "KITTENS", "OBSID": "KITTENS-0", "DATALAB": "PUPPIES-0-0"})
    fix_igrins(fits)
    assert (fits[0].header['DATALAB'] == "PUPPIES-0-0")


def test_fix_datalab():
    fits = mock_fits({"INSTRUME": "IGRINS", "GEMPRGID": "KITTENS", "OBSID": "KITTENS-0"})
    assert(fix_igrins(fits))
    assert (fits[0].header['DATALAB'] == "KITTENS-0-0")


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


class MockAstroData(object):
    def __init__(self):
        self.header = dict()


def _build_test_ad():
    ret = list()
    ret.append(MockAstroData())
    return ret


def test_alopeke_zorro_ra_cleaner():
    ad = _build_test_ad()
    ad[0].header['CTYPE2'] = 'RA--TAN'
    ret = fix_zorro_or_alopeke(ad, 'Alopeke', 'Gemini-North')
    assert(ad[0].header['CTYPE2'] == 'RA---TAN')
    assert ret
