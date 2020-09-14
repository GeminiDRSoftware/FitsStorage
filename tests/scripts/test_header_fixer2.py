
from fits_storage.scripts.header_fixer2 import fix_zorro_or_alopeke


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
