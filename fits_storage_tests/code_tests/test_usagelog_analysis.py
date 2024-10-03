from fits_storage.server.orm.usagelog_analysis import *


def test_usagelog_analysis_orm_init():
    ula = UsageLogAnalysis(1)
    assert ula.usagelog_id == 1
    assert ula.url_score == 0
    assert ula.agent_score == 0
    assert ula.referer_score == 0
    assert ula.total_score == 0


def test_score_uri():
    tests = {'foo/FLAT/FLAT/bar': 6,
             'foo/FLAT/dayCal/bar': 0,
             'foo/bar': 0,
             'foo/FLAT/ARC/OBJECT': 3,
             'foo/science/ARC': 0,  # We could flag this as inappropriate...
             }

    for url, score in tests.items():
        assert score_uri(url) == score


def test_score_agent():
    assert score_agent("") == 0


def test_score_referer():
    assert score_referrer("") == 0
