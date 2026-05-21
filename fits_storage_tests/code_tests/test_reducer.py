from fits_storage_tests.code_tests.helpers import get_test_config
get_test_config()

from fits_storage_tests.code_tests.helpers import make_empty_testing_db_env

from sqlalchemy import select
from fits_storage import utcnow

from fits_storage.db import sessionfactory
from fits_storage.config import get_config
from fits_storage.logger_dummy import DummyLogger

from fits_storage.server.reducer import Reducer
from fits_storage.server.orm.monitoring import Monitoring

class dummyad(object):
    phu = {'PROCSOFT': 'procsoft', 'PROCSVER': 'procsver', 'PROCTAG': 'proctag'}
    exts = []
    filename = ''
    id = 1

    def __iter__(self):
        for i in self.exts:
            yield i

    def data_label(self):
        return 'dummy_data_label'

    def ut_datetime(self):
        return utcnow()

class dummyrqe(object):
    pass

def getmons(session, header_id, keyword):
    stmt = select(Monitoring).where(Monitoring.header_id == header_id)

    if keyword is not None:
        stmt = stmt.where(Monitoring.keyword == keyword)

    mons = session.execute(stmt).scalars().all()

    return mons

def test_reducer_add_monitoring_value(tmp_path):
    make_empty_testing_db_env(tmp_path)
    fsc = get_config()
    fsc.config['reducer_upload_url'] = ''
    session = sessionfactory()
    logger = DummyLogger()

    ad = dummyad()
    ad.phu['FOO'] = 1
    ad.phu['BAR'] = 2

    rqe = dummyrqe()

    header_id = 10

    reducer = Reducer(session, logger, rqe)
    reducer.header_id = header_id

    # There should be no monitoring entries as the database should be empty
    mons = getmons(session, header_id, None)
    assert len(mons) == 0

    # Add a FOO value
    reducer._add_monitoring_value(ad, 'FOO')

    # Should be exactly 1, and it should be FOO = 1
    mons = getmons(session, header_id, None)
    assert len(mons) == 1
    assert mons[0].keyword == 'FOO'
    assert mons[0].value_int == 1

    # Add a BAR value
    reducer._add_monitoring_value(ad, 'BAR')

    # Check we have values for FOO and BAR
    mons = getmons(session, header_id, 'FOO')
    assert len(mons) == 1
    assert mons[0].keyword == 'FOO'
    assert mons[0].value_int == 1
    mons = getmons(session, header_id, 'BAR')
    assert len(mons) == 1
    assert mons[0].keyword == 'BAR'
    assert mons[0].value_int == 2

    # Add FOO for another header_id...
    reducer.header_id = header_id + 1
    reducer._add_monitoring_value(ad, 'FOO')

    # Check it's there and is the only entry for hid+1 ...
    mons = getmons(session, header_id+1, None)
    assert len(mons) == 1
    assert mons[0].keyword == 'FOO'
    assert mons[0].value_int == 1

    # Update FOO in the new header_id
    ad.phu['FOO'] = 3
    reducer._add_monitoring_value(ad, 'FOO')

    # Check it updated
    mons = getmons(session, header_id + 1, None)
    assert len(mons) == 1
    assert mons[0].keyword == 'FOO'
    assert mons[0].value_int == 3

    # Check FOO and BAR are still there on the original header_id
    mons = getmons(session, header_id, 'FOO')
    assert len(mons) == 1
    assert mons[0].keyword == 'FOO'
    assert mons[0].value_int == 1
    mons = getmons(session, header_id, 'BAR')
    assert len(mons) == 1
    assert mons[0].keyword == 'BAR'
    assert mons[0].value_int == 2

    # And that BAR is not there on the +1 header_id
    mons = getmons(session, header_id+1, 'BAR')
    assert len(mons) == 0
