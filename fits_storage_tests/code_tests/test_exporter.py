import datetime
import os.path

import requests

from fits_storage_tests.code_tests.helpers import make_empty_testing_db_env
from fits_storage.db import sessionfactory
from fits_storage.config import get_config
from fits_storage.logger import DummyLogger

from fits_storage.server.exporter import Exporter
from fits_storage.queues.orm.exportqueueentry import ExportQueueEntry


class dummy_qe(object):
    pass


def test_reset():
    exp = Exporter(None, DummyLogger())
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
    exp = Exporter(None, DummyLogger())

    eqe = dummy_qe()
    eqe.filename = 'N20200127S0023.fits.bz2'
    eqe.destination = 'https://archive.gemini.edu'

    exp._get_destination_file_info(eqe)

    assert exp.destination_md5 == 'e781568aff61e671dce3e4ca38cd1323'
    assert exp.destination_ingest_pending is False


class DummyRequestsResponse(object):
    # A dummy object to simulate a response from requests.post
    def __init__(self, bad=False):
        self.status_code = 500 if bad else 200
        self.text = ""


class DummyRequestsSession(object):
    # This is a dummy class we inject into the exporter instance to replace
    # the requests.Session() instance. It needs to support post() which should
    # return something that looks like a suitable response
    def __init__(self, bad=False, timeout=False):
        self.bad = bad
        self.timeout = timeout

    def post(self, *kw, **kwargs):
        if self.timeout:
            raise requests.Timeout
        return DummyRequestsResponse(bad=self.bad)


def test_destination_server_failure_handling(tmpdir):
    # This tests the code that delays all exports to a given server if that
    # server returns an error code. Also tests requests exception response
    make_empty_testing_db_env(tmpdir)
    fsc = get_config()
    session = sessionfactory()

    # Put some dummy files in storage root, so that exporter can open them.
    for fn in ['file1.bz2', 'file2.bz2']:
        fpfn = os.path.join(fsc.storage_root, fn)
        with open(fpfn, 'w') as f:
            f.write('testing')

    # We do this whole thing twice, first to test bad status code response,
    # then to test requests raising an exception.

    for failmode in ['badstatus', 'exception']:
        # Make some export queue entry items
        eqe1 = ExportQueueEntry('file1.bz2', '', 'destination1')
        eqe2 = ExportQueueEntry('file2.bz2', '', 'destination1')
        eqe3 = ExportQueueEntry('file1.bz2', '', 'destination2')

        session.add(eqe1)
        session.add(eqe2)
        session.add(eqe3)
        session.commit()

        # Get the IDs for future reference
        eqe1_id = eqe1.id
        eqe2_id = eqe2.id
        eqe3_id = eqe3.id

        # Instantiate an exporter. No logging for now.
        exp = Exporter(session, DummyLogger())

        # Patch in a dummy requests session, set to fail
        if failmode == 'badstatus':
            exp.rs = DummyRequestsSession(bad=True)
        elif failmode == 'exception':
            exp.rs = DummyRequestsSession(timeout=True)
        else:
            raise ValueError('Bad failmode value')

        # Shortcut directly to the exporter file_transfer method
        exp.eqe = eqe1
        now = datetime.datetime.utcnow()
        exp.file_transfer()

        eqe1 = session.get(ExportQueueEntry, eqe1_id)
        eqe2 = session.get(ExportQueueEntry, eqe2_id)
        eqe3 = session.get(ExportQueueEntry, eqe3_id)

        # Check failure states
        assert eqe1.failed is True
        assert eqe2.failed is False
        assert eqe3.failed is False

        if failmode == 'badstatus':
            assert eqe1.error == 'Bad HTTP status: 500 from upload post to ' \
                                 'url: destination1/upload_file/file1.bz2'
        elif failmode == 'exception':
            assert eqe1.error == 'Timeout posting to: ' \
                                 'destination1/upload_file/file1.bz2'

        # Check after values
        assert (eqe1.after - now).total_seconds() > 250
        assert (eqe2.after - now).total_seconds() > 250
        assert (eqe3.after - now).total_seconds() <= 0

        # Clean up for next failmode test
        session.delete(eqe1)
        session.delete(eqe2)
        session.delete(eqe3)
        session.commit()
