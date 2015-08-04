import pytest
import os

import fits_storage.utils.gemini_fits_validator as validator
from astrodata import AstroData

FILES_TO_TEST = (
# ABU
    ('abu01aug16_001.fits.bz2',   'NOPASS'),
    ('2001aug07_abu016.fits.bz2', 'ENG'),
# bHROS
    ('S20050824S0108.fits.bz2', 'CORRECT'),
    ('S20070103S0241.fits.bz2', 'CORRECT'),
    ('S20050725S0093.fits.bz2', 'ENG'),
    ('S20050824S0061.fits.bz2', 'NOPASS'),
    ('S20051223S0146.fits.bz2', 'BAD'),
# CIRPASS
    ('2003jul01_3547.fits.bz2', 'ENG'),
    ('2003mar23_3165.fits.bz2', 'NOPASS'),
# F2
    ('S20130621S0210.fits.bz2', 'CORRECT'),
    ('S20141229S0092.fits.bz2', 'CORRECT'),
    ('S20141220S0221.fits.bz2', 'ENG'),
    ('S20091121S0054.fits.bz2', 'NOTGEMINI'),
    ('S20100205S0038.fits.bz2', 'NOPASS'),
    ('S20130719S0512.fits.bz2', 'BAD'),
# FLAMINGOS
    ('01oct05.0023.fits.bz2', 'CORRECT'),
    ('02sep04.0230.fits.bz2', 'CORRECT'),
    ('01oct03.0055.fits.bz2', 'ENG'),
    ('02oct19.0034.fits.bz2', 'ENG'),
    ('02sep03.0172.fits.bz2', 'BAD'),
# GMOS
    ('S20090531S0054.fits.bz2', 'CORRECT'),
    ('N20021103S0082.fits.bz2', 'CORRECT'),
    ('N20030427S0085.fits.bz2', 'CORRECT'),
    ('N20011022S130.fits.bz2', 'ENG'),
    ('N20010813S128.fits.bz2', 'NOPASS'),
    ('S20141223S0024.fits.bz2', 'NOPASS'),
    ('S20140822S0040.fits.bz2', 'NOPASS'),
    ('N20011020S026.fits.bz2', 'BAD'),
    ('N20010825S102.fits.bz2', 'EXCEPTION'),
# GNIRS
    ('N20100821S0250.fits.bz2', 'CORRECT'),
    ('N20100822S0047.fits.bz2', 'CORRECT'),
    ('S20070404S0044.fits.bz2', 'ENG'),
    ('43535.fits.bz2',          'ENG'),
    ('N20100923S0003.fits.bz2', 'NOPASS'),
    ('N20110616S0540.fits.bz2', 'NOPASS'),
    ('N20101226S0027.fits.bz2', 'BAD'),
# GPI
    ('S20140315S0349.fits.bz2', 'CORRECT'),
    ('S20140316S0016.fits.bz2', 'CORRECT'),
    ('S20140321S0141.fits.bz2', 'ENG'),
    ('S20140312S0445.fits.bz2', 'NOPASS'),
    ('S20140312S0468.fits.bz2', 'NOPASS'),
    ('S20140823S0050.fits.bz2', 'BAD'),
    ('S20140730E0001.fits.bz2', 'EXCEPTION'),
# GSAOI
    ('S20111217S0079.fits.bz2', 'CORRECT'),
    ('S20121220S0359.fits.bz2', 'CORRECT'),
    ('S20110104S4001.fits.bz2', 'ENG'),
    ('S20110218S4001.fits.bz2', 'NOPASS'),
    ('S20130129S0081.fits.bz2', 'NOPASS'),
    ('S20130914S0072.fits.bz2', 'NOPASS'),
    ('S20120213S0007.fits.bz2', 'BAD'),
# Hokupaa+QUIRC
    ('01apr19_015.fits.bz2', 'CORRECT'),
    ('2002APR26_299.fits.bz2', 'CORRECT'),
    ('00AUG02_221.fits.bz2', 'ENG'),
    ('01feb21_309.fits.bz2', 'NOPASS'),
    ('01feb21_534.fits.bz2', 'NOPASS'),
    ('00aug02_089.fits.bz2', 'BAD'),
# hrwfs
    ('2001dec11_0036.fits.bz2', 'CORRECT'),
    ('S20060804S0127.fits.bz2', 'CORRECT'),
    ('S20060801S0015.fits.bz2', 'ENG'),
    ('2002may18_1.5.fits.bz2', 'NOPASS'),
    ('2003jul30_0012_bias.fits.bz2', 'NOPASS'),
    ('S20031218S0327.fits.bz2', 'NOPASS'),
    ('S20020420S0028.fits.bz2', 'BAD'),
    ('S20021227S0041.fits.bz2', 'EXCEPTION'),
# MICHELLE
    ('N20031212S0212.fits.bz2', 'CORRECT'),
    ('N20050622S0025.fits.bz2', 'CORRECT'),
    ('N20030227S0173.fits.bz2', 'ENG'),
    ('N20030616S0134.fits.bz2', 'NOPASS'),
    ('N20030616S0135b.fits.bz2', 'NOPASS'),
    ('N20050805S0215.fits.bz2', 'BAD'),
    ('N20080506S0085.fits.bz2', 'EXCEPTION'),
# NICI
    ('S20081119S0090.fits.bz2', 'CORRECT'),
    ('S20130714S0134.fits.bz2', 'CORRECT'),
    ('S20130621S0040.fits.bz2', 'ENG'),
    ('S20090412S0023.fits.bz2', 'NOPASS'),
    ('S20100406S0691.fits.bz2', 'NOPASS'),
    ('S20100407S0036.fits.bz2', 'BAD'),
# NIFS
    ('N20060722S0027.fits.bz2', 'CORRECT'),
    ('N20120612S0301.fits.bz2', 'CORRECT'),
    ('N20051025S0103.fits.bz2', 'ENG'),
    ('N20060206S0274.fits.bz2', 'NOPASS'),
    ('N20060208S0236.fits.bz2', 'NOPASS'),
    ('N20130605S0357.fits.bz2', 'NOPASS'),
    ('N20051109S0113.fits.bz2', 'BAD'),
# NIRI
    ('N20021229S0090.fits.bz2', 'CORRECT'),
    ('N20081223S0361.fits.bz2', 'CORRECT'),
    ('N20120911S0139.fits.bz2', 'CORRECT'),
    ('00sep10_0108.fits.bz2', 'ENG'),
    ('N20141210S0677.fits.bz2', 'ENG'),
    ('N20020411S0259.fits.bz2', 'NOPASS'),
    ('N20030912S0897.fits.bz2', 'NOPASS'),
    ('N20010519S088.fits.bz2', 'NOPASS'),
    ('N20130907S0898_dark.fits.bz2', 'NOPASS'),
    )

#@pytest.fixture()
#def case_iterator(testfile_path):
#    print testfile_path
#    # nobz2lst = []
#    # for f in FILES_TO_TEST:
#    #    path = testfile_path(f)
#    #    filename = os.path.basename(path)
#    #    add_to_ingestqueue(session, dummy_logger, filename, os.path.dirname(path))
#    #    nobz2lst.append(filename)
#    #
#    # yield nobz2lst

@pytest.fixture()
def evaluator(request):
    return validator.AstroDataEvaluator()

class TestEvaluator:
    @pytest.mark.parametrize("input,expected", FILES_TO_TEST)
    def test_evaluate(self, evaluator, testfile_path, input, expected):
        ad_object = AstroData(testfile_path(input))
        assert evaluator.evaluate(ad_object).code == expected
