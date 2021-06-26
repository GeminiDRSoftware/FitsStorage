from datetime import datetime, timedelta

import pytest
import sqlalchemy

import fits_storage
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.file import File
from gemini_obs_db.header import Header
from fits_storage.orm.qastuff import QAreport, QAmetricIQ, QAmetricZP, QAmetricSB, QAmetricPE
from fits_storage.web import calmgr
from fits_storage.web.qastuff import qareport, qametrics, qaforgui
from tests.file_helper import setup_mock_file_stuff
from tests.web_helper import MockContext


@pytest.mark.usefixtures("rollback")
def test_qareport(session, monkeypatch):
    try:
        mock_context = MockContext(session, method='GET')

        mock_context.raw_data = b'[{"hostname": "hostname", "userid": "userid", "processid": 5,' \
                                b'"executable": "executable", "software": "software", "software_version": ' \
                                b'"software_version", "context": "context", ' \
                                b'"qametric": [{"filename": "filename", "datalabel": "datalabel", ' \
                                b'"detector": "detector", "sb":{"mag": 50, "mag_std": 50, ' \
                                b'"electrons": 1000, "electrons_std": 1000, "nsamples":100,' \
                                b'"percentile_band": 50, "comment": ["comment1", "comment2"]}}]}]'

        def _mock_get_context(initialize=True):
            return mock_context

        monkeypatch.setattr(fits_storage.web.qastuff, "get_context", _mock_get_context)
        monkeypatch.setattr(sqlalchemy.orm.session.Session, "commit", sqlalchemy.orm.session.Session.flush)

        qareport()

        assert(mock_context.resp.status == 200)
    finally:
        session.rollback()


def test_qametrics(session, monkeypatch):
    try:
        mock_context = MockContext(session, method='GET')

        def _mock_get_context(initialize=True):
            return mock_context

        monkeypatch.setattr(fits_storage.web.qastuff, "get_context", _mock_get_context)
        monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

        setup_mock_file_stuff(monkeypatch)

        f = File('foo.fits')
        session.add(f)
        session.flush()
        df = DiskFile(f, 'foo.fits', '')
        df.canonical = True
        session.add(df)
        session.flush()
        h = Header(df)
        h.ut_datetime = datetime.now()
        h.data_label = 'datalabel'
        session.add(h)
        session.flush()
        qar = QAreport()
        session.add(qar)
        session.flush()
        iq = QAmetricIQ(qar)
        iq.datalabel = 'datalabel'
        session.add(iq)
        session.flush()
        zp = QAmetricZP(qar)
        zp.datalabel = 'datalabel'
        session.add(zp)
        session.flush()
        sb = QAmetricSB(qar)
        sb.datalabel = 'datalabel'
        session.add(sb)
        session.flush()
        pe = QAmetricPE(qar)
        pe.datalabel = 'datalabel'
        session.add(pe)
        session.flush()

        qametrics(['iq', 'zp', 'sb', 'pe'])

        assert(mock_context.resp.status == 200)
        assert('Datalabel, filename, detector, filter,' in mock_context.resp.stuff)
    finally:
        session.rollback()


def test_qaforgui(session, monkeypatch):
    try:
        mock_context = MockContext(session, method='GET')

        def _mock_get_context(initialize=True):
            return mock_context

        monkeypatch.setattr(fits_storage.web.qastuff, "get_context", _mock_get_context)
        monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)

        setup_mock_file_stuff(monkeypatch)

        f = File('foo.fits')
        session.add(f)
        session.flush()
        df = DiskFile(f, 'foo.fits', '')
        df.canonical = True
        session.add(df)
        session.flush()
        h = Header(df)
        h.ut_datetime = datetime.now()
        h.data_label = 'datalabel'
        h.types = str(('PROCESSED', 'CHEESE'))
        session.add(h)
        session.flush()
        qar = QAreport()
        session.add(qar)
        session.flush()
        iq = QAmetricIQ(qar)
        iq.datalabel = 'datalabel'
        iq.comment = 'comment'
        session.add(iq)
        session.flush()
        zp = QAmetricZP(qar)
        zp.datalabel = 'datalabel'
        zp.comment = 'comment'
        zp.percentile_band = '50'
        zp.cloud = 50
        zp.cloud_std = 5
        zp.mag = 10
        zp.mag_std = 1
        session.add(zp)
        session.flush()
        sb = QAmetricSB(qar)
        sb.datalabel = 'datalabel'
        sb.comment = 'comment'
        session.add(sb)
        session.flush()
        pe = QAmetricPE(qar)
        pe.datalabel = 'datalabel'
        pe.comment = 'comment'
        session.add(pe)
        session.flush()

        fromdt = h.ut_datetime - timedelta(days=2)
        todt = h.ut_datetime + timedelta(days=2)
        qaforgui('%s-%s' % (fromdt.strftime('%Y%m%d'), todt.strftime('%Y%m%d')))

        assert(mock_context.resp.status == 200)
        assert("'types': ('PROCESSED', 'CHEESE')" in mock_context.resp.stuff)
    finally:
        session.rollback()
