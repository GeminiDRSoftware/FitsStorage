from fits_storage_tests.code_tests.helpers import get_test_config
get_test_config()

from fits_storage_tests.code_tests.helpers import make_empty_testing_db_env

from fits_storage.db import sessionfactory
from fits_storage.config import get_config
from fits_storage.logger_dummy import DummyLogger

from fits_storage.queues.orm.reducequeentry import ReduceQueueEntry
from fits_storage.queues.queue.reducequeue import ReduceQueue

from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header

def test_reducequeue(tmp_path):
    make_empty_testing_db_env(tmp_path)
    fsc = get_config()
    session = sessionfactory()
    logger = DummyLogger()

    # Testing with SQLite, where the filenames column is just text rather than
    # a list. That's fine for the test because we don't use filename*s* at all.
    rqe = ReduceQueueEntry('N20100101S0001.fits')
    rqe.mem_gb = 8
    session.add(rqe)
    session.commit()

    rqe = ReduceQueueEntry('N20200101S0001.fits')
    rqe.mem_gb = 8
    session.add(rqe)
    session.commit()

    rq = ReduceQueue(session, logger)
    rq.server_gbs = 10
    host = rq.host
    assert host is not None

    pop1_rqe = rq.pop()
    assert pop1_rqe is not None
    assert pop1_rqe.inprogress is True

    # Should be most recent one
    assert pop1_rqe.filenames == 'N20200101S0001.fits'

    # Should have been set when it was popped
    assert pop1_rqe.host == host

    pop2_rqe = rq.pop()
    # Not enough memory to do both
    assert pop2_rqe is None

    session.delete(pop1_rqe)
    session.commit()

    pop2_rqe = rq.pop()
    assert pop2_rqe is not None
    assert pop2_rqe.inprogress is True
    assert pop2_rqe.host == host
    assert pop2_rqe.filenames == 'N20100101S0001.fits'

    session.delete(pop2_rqe)
    session.commit()

    pop3_rqe = rq.pop()
    assert pop3_rqe is None