from fits_storage.gemini_metadata_utils import gemini_telescope


def test_gemini_telescope():
    gnvals = ['Gemini-North', 'gemini-north', 'gemini_north', 'geminiNorth']
    gsvals = ['Gemini-South', 'gemini-south', 'gemini_south', 'Geminisouth']
    badvals = ['Unknown', '', 1, None]

    for i in gnvals:
        assert gemini_telescope(i) == 'Gemini-North'

    for i in gsvals:
        assert gemini_telescope(i) == 'Gemini-South'

    for i in badvals:
        assert gemini_telescope(i) is None
