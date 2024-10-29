from fits_storage_tests.code_tests.helpers import get_test_config
get_test_config()

from fits_storage.db.selection import Selection


# This data block is a list of 3-element tuples. The elements in the tuples are:
# * list of "things" from the URL
# * selection item key
# * selection item value
getselection_data = [
    (['Gemini-North'], 'telescope', 'Gemini-North'),
    (['science'], 'observation_class', 'science'),
    (['OBJECT'], 'observation_type', 'OBJECT'),
    (['N20001122S1234.fits'], 'filename', 'N20001122S1234.fits'),
    (['BIAS'], 'observation_type', 'BIAS'),
    (['bias'], 'caltype', 'bias'),
    (['Quick-Look'], 'processing', 'Quick-Look'),
    (['RAW'], 'reduction', 'RAW'),
    (['B600'], 'disperser', 'B600'),
    (['filepre=hello'], 'filepre', 'hello'),
    (['Pass'], 'qa_state', 'Pass'),
    (['Fail'], 'qa_state', 'Fail'),
    (['GMOS-N'], 'inst', 'GMOS-N'),
    (['GMOS'], 'inst', 'GMOS'),
    (['night=20001122'], 'night', '20001122'),
    (['nightrange=20001020-20001122'], 'nightrange', '20001020-20001122'),
    (['date=20001122'], 'date', '20001122'),
    (['daterange=20001020-20001122'], 'daterange', '20001020-20001122'),
    (['20121212'], 'night', '20121212'),
    (['20100101-20101122'], 'nightrange', '20100101-20101122'),
]


def test_getselection():
    get_test_config()

    for (thinglist, key, value) in getselection_data:
        selection = Selection(thinglist)
        assert selection._seldict[key] == value
