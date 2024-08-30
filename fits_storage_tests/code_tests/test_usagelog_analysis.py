from fits_storage.server.usagelog_analysis import score_url


def test_score_url():
    tests = {'foo/FLAT/FLAT/bar': 6,
             'foo/FLAT/dayCal/bar': 0,
             'foo/bar': 0,
             'foo/FLAT/ARC/OBJECT': 3,
             'foo/science/ARC': 0,  # We could flag this as inappropriate...
             }

    for url, score in tests.items():
        assert score_url(url) == score
