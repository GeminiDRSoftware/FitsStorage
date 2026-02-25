import os.path

from fits_storage.server.reduce_on_ingest import ReduceOnIngest
from fits_storage.core.orm.header import Header
from fits_storage.logger_dummy import DummyLogger

from fits_storage_tests.code_tests.helpers import get_test_config, make_diskfile


class DummyReduceQueue(object):
    def __init__(self):
        self.added = []
        self.actions = []
    def add(self, item, **kws):
        self.added.append(item)
        self.actions.append(kws)

def compareactions(a, b):
    ignore = ['mem_gb']
    for i in a.keys():
        if i in ignore:
            continue
        if i not in b.keys():
            return False
        if a[i] != b[i]:
            return False
    return True

def test_reduce_on_ingest(tmp_path):
    get_test_config()

    diskfile = make_diskfile('N20200127S0023.fits.bz2', tmp_path)
    header = Header(diskfile)

    rules_fpfn = os.path.join(tmp_path, 'roi_rules.json')
    rules_text = """[
  [{"instrument": "NIRI", "observation_type": "OBJECT", "onlynew": true},
   {"recipe": "testrecipe1", "capture_monitoring": true, "tag": "testtag", "initiatedby": "testib", "intent": "Science-Quality"}],
     [{"instrument": "NIRI", "observation_type": "OBJECT", "onlynew": false},
   {"recipe": "testrecipe2", "capture_monitoring": true, "tag": "testtag", "initiatedby": "testib", "intent": "Science-Quality"}]
]
"""
    with open(rules_fpfn, 'w') as f:
        f.write(rules_text)

    rq = DummyReduceQueue()
    roi = ReduceOnIngest(rules_file=rules_fpfn, session=None, logger=DummyLogger())

    roi(diskfile, True, header=header, rq=rq)
    assert len(rq.added) == 2
    assert rq.added[0] == ['N20200127S0023.fits.bz2']
    assert rq.added[1] == ['N20200127S0023.fits.bz2']
    assert compareactions(rq.actions[0], {"recipe": "testrecipe1", "capture_monitoring": True, "tag": "testtag", "initiatedby": "testib", "intent": "Science-Quality"})
    assert compareactions(rq.actions[1], {"recipe": "testrecipe2", "capture_monitoring": True, "tag": "testtag", "initiatedby": "testib", "intent": "Science-Quality"})

    roi(diskfile, False, header=header, rq=rq)
    assert len(rq.added) == 3
    assert compareactions(rq.actions[2], {"recipe": "testrecipe2", "capture_monitoring": True, "tag": "testtag", "initiatedby": "testib", "intent": "Science-Quality"})

