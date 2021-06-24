from datetime import datetime

import pytest

from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.file import File
from gemini_obs_db.header import Header
from fits_storage.web.api import lookup_diskfile, valid_pair, map_release


@pytest.mark.usefixtures("rollback")
@pytest.mark.slow
def test_process(session):
    session.rollback()
    filename = "N20191010S0144.fits"
    path = ""
    file = File(filename)
    session.add(file)
    session.flush()
    df = DiskFile(file, filename, path)
    session.add(df)
    session.flush()
    header = Header(df)
    session.add(header)
    session.flush()
    query = dict()
    query['data_label'] = 'GN-CAL20191010-23-057'
    label, df = lookup_diskfile(session, query)
    assert(label == 'GN-CAL20191010-23-057')
    assert(df.filename == filename)
    query = { "filename": filename }
    label, df = lookup_diskfile(session, query)
    assert(label == filename)
    assert(df.filename == filename)


def test_valid_pair():
    assert(valid_pair((1, 2)))
    assert(not valid_pair((1, None)))


def test_map_release():
    ret = map_release('2019-1-1')
    assert(len(ret) == 1)
    assert(len(ret[0]) == 2)
    assert(ret[0][0] == 'RELEASE')
    assert(ret[0][1] == '2019-1-1')
    with pytest.raises(ValueError):
        assert map_release('thisisnotatest')
