from fits_storage.server.exporter import Exporter

class dummy_qe(object):
    pass

def test_reset():
    exp = Exporter(None, None)
    exp.got_destination_info = True
    exp.destination_md5 = 'e781568aff61e671dce3e4ca38cd1323'
    exp.destination_ingest_pending = False
    exp.eqe = 'something'
    exp.df = 'something'

    exp.reset()
    assert exp.got_destination_info is None
    assert exp.destination_ingest_pending is None
    assert exp.destination_md5 is None
    assert exp.eqe is None
    assert exp.df is None

def test_get_destination_file_info():
    exp = Exporter(None, None)

    eqe = dummy_qe()
    eqe.filename = 'N20200127S0023.fits.bz2'
    eqe.destination = 'https://archive.gemini.edu'

    exp._get_destination_file_info(eqe)

    assert exp.destination_md5 == 'e781568aff61e671dce3e4ca38cd1323'
    assert exp.destination_ingest_pending is False
