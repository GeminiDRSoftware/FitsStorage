import datetime
from fits_storage.server.tapeutils import FileOnTapeHelper

def test_1():
    foth = FileOnTapeHelper(tapeserver='hbffitstape-lp2')
    foth.populate_cache('N20220501S0001')

    assert foth._queried == ['N20220501S0001']
    assert len(foth._cache) == 2
    assert foth._cache[0]['filename'] == 'N20220501S0001.fits.bz2'
    assert foth.check_results('N20220501S0001.fits.bz2') == {113, 114}

def test_2():
    foth = FileOnTapeHelper(tapeserver='hbffitstape-lp2')

    # This should trigger an API query for that whole night
    assert foth.check_file('N20220501S0001.fits') == {113, 114}

    # Subsequent successful queries for that night should be very fast
    start = datetime.datetime.now()
    assert foth.check_file('N20220501S0002.fits') == {113, 114}
    end = datetime.datetime.now()
    jiffy = datetime.timedelta(milliseconds=10)
    assert end - start < jiffy