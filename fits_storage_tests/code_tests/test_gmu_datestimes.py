from fits_storage.gemini_metadata_utils.datestimes import *

import dateutil.parser

simple_good = {'20010101-20020202': ('20010101', '20020202'),
               '20111231-20120101': ('20111231', '20120101')}

simple_bad = {'20012201-20020202': None,
              '20111231-20120142': None}

iso_good = {'20001122T012345.67-20010102T112233.44':
                ('20001122T012345.67', '20010102T112233.44'),
            '20001122T012345-20010102T112233':
                ('20001122T012345', '20010102T112233'),
            '20001122T012345.-20010102T112233.44456':
                ('20001122T012345.', '20010102T112233.44456')}

iso_bad = {'20002122T012345.67-20010102T112233.44': None,
           '20001142T012345-20010102T112233': None,
           '20001122T016345.-20010102T112233.44456': None}


def test_simple_daterange_regex():
    for i in simple_good.keys():
        start, end = simple_good[i]
        m = simple_daterange_cre.match(i)
        assert m is not None
        assert m.group('start') == start
        assert m.group('end') == end

    for i in simple_bad.keys():
        m = simple_daterange_cre.match(i)
        assert m is None


def test_iso_daterange_regex():
    for i in iso_good.keys():
        start, end = iso_good[i]
        m = iso_daterange_cre.match(i)
        assert m is not None
        assert m.group('start') == start
        assert m.group('end') == end

    for i in iso_bad.keys():
        m = iso_daterange_cre.match(i)
        assert m is None


def test_daterange_simple_strings():
    for i in simple_good.keys():
        start, end = simple_good[i]
        result = gemini_daterange(i)
        assert result == f"{start}-{end}"


def test_daterange_simple_asdates():
    for i in simple_good.keys():
        start, end = simple_good[i]
        start = dateutil.parser.parse(start).replace(tzinfo=None).date()
        end = dateutil.parser.parse(end).replace(tzinfo=None).date()

        result = gemini_daterange(i, as_dates=True)
        assert result == (start, end)


def test_daterange_iso():
    for i in iso_good.keys():
        start, end = iso_good[i]
        start = dateutil.parser.parse(start).replace(tzinfo=None)
        end = dateutil.parser.parse(end).replace(tzinfo=None)

        string = gemini_daterange(i)
        assert string == i

        result = gemini_daterange(i, as_dates=True)
        assert result == (start, end)