from fits_storage_tests.code_tests.helpers import get_test_config
get_test_config()

from fits_storage.db.selection.get_selection import from_url_things


# This data block is a list of 3-element tuples. The elements in the tuples are:
# * list of "things" from the URL
# * selection item key
# * selection item value
getselection_data = [
    # 'getselection_tests' items:
    (['Gemini-North'], 'telescope', 'Gemini-North'),
    (['gemini-north'], 'telescope', 'Gemini-North'),
    (['N20001122S1234.fits'], 'filename', 'N20001122S1234.fits'),
    (['OBJECT'], 'observation_type', 'OBJECT'),
    (['BIAS'], 'observation_type', 'BIAS'),
    (['science'], 'observation_class', 'science'),
    (['bias'], 'caltype', 'bias'),
    (['Quick-Look'], 'processing', 'Quick-Look'),
    (['RAW'], 'reduction', 'RAW'),
    (['B600'], 'disperser', 'B600'),
    (['1.5arcsec'], 'focal_plane_mask', '1.5arcsec'),
    (['2x2'], 'binning', '2x2'),
    (['high'], 'gain', 'high'),
    (['fast'], 'readspeed', 'fast'),
    (['Deep'], 'welldepth', 'Deep'),
    (['NodAndShuffle'], 'readmode', 'NodAndShuffle'),
    (['GMOS-N'], 'inst', 'GMOS-N'),
    (['GMOS'], 'inst', 'GMOS'),
    # 'getselection_key_value' items. This just tests getting them, not parsing
    (['filename=asdf'], 'filename', 'asdf'),
    (['filepre=sdfg'], 'filepre', 'sdfg'),
    (['disperser=dfgh'], 'disperser', 'dfgh'),
    (['camera=fghj'], 'camera', 'fghj'),
    (['mask=ghjk'], 'focal_plane_mask', 'ghjk'),
    (['pupil_mask=hjkl'], 'pupil_mask', 'hjkl'),
    (['filter=zxcv'], 'filter', 'zxcv'),
    (['Filter=xcvb'], 'filter', 'xcvb'),
    (['az=cvbn'], 'az', 'cvbn'),
    (['Az=vbnm'], 'az', 'vbnm'),
    (['azimuth=qwer'], 'az', 'qwer'),
    (['Azimuth=wert'], 'az', 'wert'),
    (['el=erty'], 'el', 'erty'),
    (['El=rtyu'], 'el', 'rtyu'),
    (['elevation=tyui'], 'el', 'tyui'),
    (['Elevation=uiop'], 'el', 'uiop'),
    (['ra=qaz'], 'ra', 'qaz'),
    (['RA=wsx'], 'ra', 'wsx'),
    (['dec=edc'], 'dec', 'edc'),
    (['Dec=rfv'], 'dec', 'rfv'),
    (['sr=tgb'], 'sr', 'tgb'),
    (['SR=yhn'], 'sr', 'yhn'),
    (['crpa=ujm'], 'crpa', 'ujm'),
    (['CRPA=okm'], 'crpa', 'okm'),
    (['cenwlen=ijn'], 'cenwlen', 'ijn'),
    (['exposure_time=uhb'], 'exposure_time', 'uhb'),
    (['coadds=ygv'], 'coadds', 'ygv'),
    (['publication=tfc'], 'publication', 'tfc'),
    (['PIname=rdx'], 'PIname', 'rdx'),
    (['ProgramText=esz'], 'ProgramText', 'esz'),
    (['raw_cc=qwe'], 'raw_cc', 'qwe'),
    (['raw_iq=wer'], 'raw_iq', 'wer'),
    (['gain=ert'], 'gain', 'ert'),
    (['readspeed=rty'], 'readspeed', 'rty'),
    (['night=20001122'], 'night', '20001122'),
    (['nightrange=20001020-20001122'], 'nightrange', '20001020-20001122'),
    (['date=20001122'], 'date', '20001122'),
    (['daterange=20001020-20001122'], 'daterange', '20001020-20001122'),
    (['20121212'], 'night', '20121212'),
    (['20100101-20101122'], 'nightrange', '20100101-20101122'),
    # 'getselection_simple_associations' items
    (['warnings'], 'caloption', 'warnings'),
    (['missing'], 'caloption', 'missing'),
    (['requires'], 'caloption', 'requires'),
    (['takenow'], 'caloption', 'takenow'),
    (['Pass'], 'qa_state', 'Pass'),
    (['Usable'], 'qa_state', 'Usable'),
    (['Fail'], 'qa_state', 'Fail'),
    (['Win'], 'qa_state', 'Win'),
    (['NotFail'], 'qa_state', 'NotFail'),
    (['Lucky'], 'qa_state', 'Lucky'),
    (['AnyQA'], 'qa_state', 'AnyQA'),
    (['CHECK'], 'qa_state', 'CHECK'),
    (['UndefinedQA'], 'qa_state', 'UndefinedQA'),
    (['AO'], 'ao', 'AO'),
    (['NOTAO'], 'ao', 'NOTAO'),
    (['NOAO'], 'ao', 'NOAO'),
    # getselection_booleans
    (['imaging'], 'spectroscopy', False),
    (['spectroscopy'], 'spectroscopy', True),
    (['present'], 'present', True),
    (['Present'], 'present', True),
    (['notpresent'], 'present', False),
    (['NotPresent'], 'present', False),
    (['canonical'], 'canonical', True),
    (['Canonical'], 'canonical', True),
    (['notcanonical'], 'canonical', False),
    (['NotCanonical'], 'canonical', False),
    (['engineering'], 'engineering', True),
    (['notengineering'], 'engineering', False),
    (['science_verification'], 'science_verification', True),
    (['notscience_verification'], 'science_verification', False),
    (['calprog'], 'calprog', True),
    (['notcalprog'], 'calprog', False),
    (['site_monitoring'], 'site_monitoring', True),
    (['not_site_monitoring'], 'site_monitoring', False),
    (['photstandard'], 'photstandard', True),
    (['mdgood'], 'mdready', True),
    (['mdbad'], 'mdready', False),
    (['gpi_astrometric_standard'], 'gpi_astrometric_standard', True),
    (['includeengineering'], 'engineering', 'Include'),
    # getselection_detector_roi...
    (['fullframe'], 'detector_roi', 'Full Frame'),
    (['centralstamp'], 'detector_roi', 'Central Stamp'),
    (['centralspectrum'], 'detector_roi', 'Central Spectrum'),
    (['central768'], 'detector_roi', 'Central768'),
    (['central512'], 'detector_roi', 'Central512'),
    (['central256'], 'detector_roi', 'Central256'),
    (['custom'], 'detector_roi', 'Custom'),
    # Program ID, Obervation ID and Data Label handling. Note, see also
    # separate test for multiple values
    (['GN-2010A-Q-123'], 'program_id', 'GN-2010A-Q-123'),
    (['GN-2010A-Q-123-01'], 'observation_id', 'GN-2010A-Q-123-01'),
    (['GN-2010A-Q-123-01-123'], 'data_label', 'GN-2010A-Q-123-01-123'),
    (['LGS'], 'lgs', 'LGS'),
    (['NGS'], 'lgs', 'NGS'),
    (['LGS'], 'ao', 'AO'),
    (['NGS'], 'ao', 'AO'),
    (['Raw'], 'processing', 'Raw'),
    (['Quick-Look'], 'processing', 'Quick-Look'),
    (['Science-Quality'], 'processing', 'Science-Quality'),
    (['preimage'], 'pre_image', True),
    (['twilight'], 'twilight', True),
    (['nottwilight'], 'twilight', False),
    (['N201002'], 'filepre', 'N201002'),
    (['object=foo'], 'object', 'foo'),
    (['Object=bar'], 'object', 'bar'),
    (['LS'], 'mode', 'LS'),
    (['LS'], 'spectroscopy', True),
    (['MOS'], 'mode', 'MOS'),
    (['MOS'], 'spectroscopy', True),
    (['IFS'], 'mode', 'IFS'),
    (['IFS'], 'spectroscopy', True),
    # ([''], '', ''),
]


def test_from_url_things():
    get_test_config()

    for (thinglist, key, value) in getselection_data:
        print(f'Testing {thinglist} -> {key}: {value}')
        selection = from_url_things(thinglist)
        try:
            assert selection[key] == value
        except KeyError:
            print(f"Selection dict is: {selection}")
            raise

def test_openquery():
    openthings = ['GMOS']
    closedthings = ['date=20001122']

    selection = from_url_things(openthings)
    assert selection.openquery is True

    selection = from_url_things(closedthings)
    assert selection.openquery is False

def test_programobsdl_precedence():
    get_test_config()

    # prog and obs
    selection = from_url_things(['GN-2010A-Q-123', 'GN-2010A-Q-123-01'])
    assert 'program_id' not in selection.keys()
    assert 'data_label' not in selection.keys()
    assert selection['observation_id'] == 'GN-2010A-Q-123-01'

    # prog and dl
    selection = from_url_things(['GN-2010A-Q-123', 'GN-2010A-Q-123-01-123'])
    assert 'program_id' not in selection.keys()
    assert 'observation_id' not in selection.keys()
    assert selection['data_label'] == 'GN-2010A-Q-123-01-123'

    # obs and dl
    selection = from_url_things(['GN-2010A-Q-123-01', 'GN-2010A-Q-123-01-123'])
    assert 'program_id' not in selection.keys()
    assert 'observation_id' not in selection.keys()
    assert selection['data_label'] == 'GN-2010A-Q-123-01-123'

    # All 3
    selection = from_url_things(['GN-2010A-Q-123', 'GN-2010A-Q-123-01',
                                 'GN-2010A-Q-123-01-123'])
    assert 'program_id' not in selection.keys()
    assert 'observation_id' not in selection.keys()
    assert selection['data_label'] == 'GN-2010A-Q-123-01-123'


