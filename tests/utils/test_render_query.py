from datetime import datetime

import pytest

from gemini_obs_db.header import Header
from fits_storage.utils.render_query import render_query


@pytest.mark.usefixtures("rollback")
def test_render_query(session, monkeypatch):
    q = session.query(Header).filter(Header.ut_datetime == datetime(2020, 1, 1)).filter(Header.program_id == 'PROGRAM_ID')
    result = render_query(q)
    assert("FROM header \nWHERE header.ut_datetime = '2020-01-01 00:00:00' AND header.program_id = 'PROGRAM_ID'"
           in result)
    pass

