import pytest
from fits_storage.web.selection import getselection, sayselection, queryselection
from fits_storage.orm import compiled_statement
from fits_storage.gemini_metadata_utils import gemini_date, ONEDAY_OFFSET
from fits_storage import fits_storage_config

from collections import OrderedDict
from sqlalchemy import Column, Integer, or_, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.query import Query
from sqlalchemy.sql.elements import AsBoolean, BooleanClauseList
from datetime import datetime, timedelta
from types import MethodType

Base = declarative_base()


class Dummy(Base):
    __tablename__ = 'dummy'
    id = Column(Integer, primary_key = True)


_saved_use_as_archive = None


def setup_function(function):
    """ Save our use as archive state and set to 'True'.

    This makes date handling predictible
    """
    global _saved_use_as_archive
    if _saved_use_as_archive is None:
        _saved_use_as_archive = fits_storage_config.use_as_archive
    fits_storage_config.use_as_archive = True


def teardown_function(function):
    """ teardown any state that was previously setup with a setup_module
    method.
    """
    if _saved_use_as_archive is not None:
        fits_storage_config.use_as_archive = _saved_use_as_archive


@pytest.fixture(scope='session')
def query(request):
    'Creates a dummy query for tests depending on ORM'

    return Query(Dummy)


def build_query(request):
    """Creates a dummy query for tests depending on ORM.

    This is a duplicate to fix a test that called query()
    directly.  pytest does not like us using fixtures in
    that way.
    """

    return Query(Dummy)


getselection_pairs = [
    (['12345'], {'notrecognised': '12345'}),
    (['123 456'], {'notrecognised': '123 456'}),
    (['gemini-north'], {'telescope': 'Gemini-North'}),
    (['GEMINI-SOUTH'], {'telescope': 'Gemini-South'}),
    (['19990101'], {'date': '19990101'}),
    (['20491231'], {'date': '20491231'}),
    # (['today'], {'date': gemini_date('today')}),
    (['21000101'], {'notrecognised': '21000101'}),
    (['20140101-20150101'], {'daterange': '20140101-20150101'}),
    (['20140101-today'], {'daterange': '20140101-today'}),
    (['20140101-tomorrow'], {'notrecognised': '20140101-tomorrow'}),
    (['GN-CAL20150623'], {'program_id': 'GN-CAL20150623'}),
    # (['GN-CAL20150623-21', 'GN-CAL20150623'], {'program_id': 'GN-CAL20150623'}),
    # (['GN-CAL20150623-21-001', 'GN-CAL20150623'], {'program_id': 'GN-CAL20150623'}),
    (['progid=GN-CAL20150623'], {'program_id': 'GN-CAL20150623'}),
    (['GN-CAL20150623-21'], {'observation_id': 'GN-CAL20150623-21'}),
    (['obsid=GN-CAL20150623'], {'observation_id': 'GN-CAL20150623'}), # Maybe this shouldn't be allowed?
    (['GN-CAL20150623-21-001'], {'data_label': 'GN-CAL20150623-21-001'}),
    # Instrument...
    (['gMoS'],  {'inst': 'GMOS'}),
    (['gMoS-n'],  {'inst': 'GMOS-N'}),
    (['gMoS-S'],  {'inst': 'GMOS-S'}),
    (['NiRi'],  {'inst': 'NIRI'}),
    (['niFS'],  {'inst': 'NIFS'}),
    (['NicI'],  {'inst': 'NICI'}),
    (['GNirs'], {'inst': 'GNIRS'}),
    (['MichELLE'],  {'inst': 'michelle'}),
    (['phOEnix'],  {'inst': 'PHOENIX'}),
    (['teXES'],  {'inst': 'TEXES'}),
    (['Trecs'],  {'inst': 'TReCS'}),
    (['HOKUPA+quirc'],  {'inst': 'Hokupaa+QUIRC'}),
    (['HOKUPAA+++quirc'],  {'inst': 'Hokupaa+QUIRC'}),
    (['GSaoi'],  {'inst': 'GSAOI'}),
    (['osCIR'],  {'inst': 'OSCIR'}),
    (['f2'],  {'inst': 'F2'}),
    (['gPi'],  {'inst': 'GPI'}),
    (['abu'],  {'inst': 'ABU'}),
    (['bhroS'],  {'inst': 'bHROS'}),
    (['HrWfS'],  {'inst': 'hrwfs'}),
    (['fLAMINGOs'],  {'inst': 'FLAMINGOS'}),
    (['cirpass'],  {'inst': 'CIRPASS'}),
    # Test what happens when you pass more than one instrument
    (['niri', 'nici', 'trecs', 'gmos', 'nici'], {'inst': 'NICI'}),
    (['N20310101S000'], {'notrecognised': 'N20310101S000'}),
    (['N20010101S000'], {'filepre': 'N20010101S000'}),
    (['filepre=N20310101S000'], {'filepre': 'N20310101S000'}),
    (['N20990101S0001'], {'filename': 'N20990101S0001.fits'}),
    (['N20010101S0001.fits'], {'filename': 'N20010101S0001.fits'}),
    (['N20990101S0001.fits'], {'filename': 'N20990101S0001.fits'}),
    (['N21000101S0001.fits'], {'notrecognised': 'N21000101S0001.fits'}),
    (['filename=foobarbaz'], {'filename': 'foobarbaz'}),
    (['DARK'], {'observation_type': 'DARK'}),
    (['OBJECT'], {'observation_type': 'OBJECT'}),
    (['PINHOLE'], {'observation_type': 'PINHOLE'}),
    (['RONCHI'], {'observation_type': 'RONCHI'}),
    (['ronchi'], {'notrecognised': 'ronchi'}),
    (['dayCal'], {'observation_class': 'dayCal'}),
    (['science'], {'observation_class': 'science'}),
    (['dark'], {'caltype': 'dark'}),
    (['specphot'], {'caltype': 'specphot'}),
#    (['PROCESSED_FLAT'], {'reduction': 'PROCESSED_FLAT'}),
    (['R600'], {'disperser': 'R600'}),
    (['disperser=R600'], {'disperser': 'R600'}),
    (['camera=foobar'], {'camera': 'foobar'}),
    (['NS0.75arcsec'], {'focal_plane_mask': 'NS0.75arcsec'}),
    (['GN2014A#314-32'], {'focal_plane_mask': 'GN2014A#314-32'}),
    (['mask=foobar'], {'focal_plane_mask': 'foobar'}),
    (['warnings'], {'caloption': 'warnings'}),
    (['missing'], {'caloption': 'missing'}),
    (['requires'], {'caloption': 'requires'}),
    (['takenow'], {'caloption': 'takenow'}),
    (['imaging'], {'spectroscopy': False}),
    (['spectroscopy'], {'spectroscopy': True}),
    (['IFS'], {'mode': 'IFS', 'spectroscopy': True}),
    (['imaging', 'MOS'], {'mode': 'MOS', 'spectroscopy': True}),
    (['Win'], {'qa_state': 'Win'}),
    (['AnyQA'], {'qa_state': 'AnyQA'}),
    (['LGS'], {'lgs': 'LGS', 'ao': 'AO'}),
    (['NGS'], {'lgs': 'NGS', 'ao': 'AO'}),
    (['AO'], {'ao': 'AO'}),
    (['NOTAO'], {'ao': 'NOTAO'}),
    (['present'], {'present': True}),
    (['Present'], {'present': True}),
    (['notpresent'], {'present': False}),
    (['NotPresent'], {'present': False}),
    (['canonical'], {'canonical': True}),
    (['Canonical'], {'canonical': True}),
    (['notcanonical'], {'canonical': False}),
    (['NotCanonical'], {'canonical': False}),
    (['engineering'], {'engineering': True}),
    (['notengineering'], {'engineering': False}),
    (['includeengineering'], {'engineering': 'Include'}),
    (['science_verification'], {'science_verification': True}),
    (['notscience_verification'], {'science_verification': False}),
    (['filter=blah'], {'filter': 'blah'}),
    (['Filter=blah'], {'filter': 'blah'}),
    (['object=bleh'], {'object': 'bleh'}),
    (['Object=bleh'], {'object': 'bleh'}),
    (['1x2'], {'binning': '1x2'}),
    (['4x1'], {'binning': '4x1'}),
    (['6x1'], {'notrecognised': '6x1'}),
    (['photstandard'], {'photstandard': True}),
    #(['low'], {'detector_config': ['low']}),
    #(['low', 'NodAndShuffle'], {'detector_config': ['low', 'NodAndShuffle']}),
    #(['Bright', 'low', 'NodAndShuffle'], {'detector_config': ['Bright', 'low', 'NodAndShuffle']}),
    (['centralspectrum'], {'detector_roi': 'Central Spectrum'}),
    (['central256'], {'detector_roi': 'Central256'}),
    (['twilight'], {'twilight': True}),
    (['nottwilight'], {'twilight': False}),
    (['Az=kkk'], {'az': 'kkk'}),
    (['Azimuth=kkk'], {'az': 'kkk'}),
    (['El=kkk'], {'el': 'kkk'}),
    (['elevation=kkk'], {'el': 'kkk'}),
    (['RA=fjfj'], {'ra': 'fjfj'}),
    (['DEC=fjfj'], {'notrecognised': 'DEC=fjfj'}),
    (['Dec=fjfj'], {'dec': 'fjfj'}),
    (['SR=kjkj'], {'sr': 'kjkj'}),
    (['crpa=kjkj'], {'crpa': 'kjkj'}),
    (['Crpa=kjkj'], {'notrecognised': 'Crpa=kjkj'}),
    (['cenwlen=blahblah'], {'cenwlen': 'blahblah'}),
    (['exposure_time=blahblah'], {'exposure_time': 'blahblah'}),
    (['high'], {'gain': 'high'}),
    (['cols=program_id'], {'cols': 'program_id'}),
    (['sq'], {'procsci': 'sq'}),
    (['preimage'], {'pre_image': True}),
    (["zardoz"], {'notrecognised': 'zardoz'}),
]


@pytest.mark.parametrize("input,expected", getselection_pairs)
def test_getselection(input, expected):
    assert getselection(input) == expected


sayselection_pairs = [
    ({'program_id': 'GN-CAL20150623', 'cenwlen': 'blahblah', 'exposure_time': 'blahblah'},
     {' Exposure Time: blahblah', ' Program ID: GN-CAL20150623', ' Central Wavelength: blahblah'}),
    ({'inst': 'GMOS-N', 'telescope': 'Gemini-North', 'date': '20991231'},
     {' Instrument: GMOS-N', ' Date: 20991231', ' Telescope: Gemini-North'}),
    ({'spectroscopy': True, 'qa_state': 'Win', 'ao': 'NOTAO', 'lgs': 'NGS'},
     '; Spectroscopy; QA State: Win (Pass or Usable); No Adaptive Optics in beam; NGS'),
    # O detector_config no longer exists?
    # ({'spectroscopy': False, 'qa_state': 'Something', 'detector_config': ['Bright', 'low']},
    #  '; Imaging; QA State: Something; Detector Config: Bright+low'),
    ({'notrecognised': 'Foobar'},
     ". WARNING: I didn't understand these (case-sensitive) words: Foobar"),
    ({'program_id': 'GN-CAL20150623', 'notrecognised': 'Foobar'},
     "; Program ID: GN-CAL20150623. WARNING: I didn't understand these (case-sensitive) words: Foobar"),
    ({'site_monitoring': True}, {' Is Site Monitoring Data'}),
    ]


@pytest.mark.parametrize("input,expected", sayselection_pairs)
def test_sayselection(input, expected):
    if isinstance(expected, set):
        split = set(x for x in sayselection(input).split(';') if x)
        assert split == expected
    else:
        assert sayselection(input) == expected

from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.file import File
from fits_storage.orm.footprint import Footprint
from fits_storage.orm.header import Header
from fits_storage.orm.photstandard import PhotStandardObs

queryselection_pair_source = (
    ('present', DiskFile.present),
    ('canonical', DiskFile.canonical),
    ('science_verification', Header.science_verification),
    ('engineering', Header.engineering),
    (('program_id', 'foobar'), Header.program_id),
    (('observation_id', 'foobar'), Header.observation_id),
    (('data_label', 'foobar'), Header.data_label),
    (('observation_type', 'foobar'), Header.observation_type),
    (('observation_class', 'foobar'), Header.observation_class),
    (('reduction', 'foobar'), Header.reduction),
    (('telescope', 'foobar'), Header.telescope),
    (('inst', 'NIRI'), Header.instrument),
    (('inst', 'GMOS'), or_(Header.instrument == 'GMOS-N', Header.instrument == 'GMOS-S')),
    (('filename', 'foobar'), File.name),
    (('filelist', ('foo', 'bar', 'baz')), File.name.in_),
    ('spectroscopy', Header.spectroscopy),
    (('mode', 'foobar'), Header.mode),
    (('binning', '1x1'), Header.detector_binning),
    (('filter', 'foobar'), Header.filter_name),
    ('photstandard', and_(Footprint.header_id == Header.id, PhotStandardObs.footprint_id == Footprint.id)),
    # The following are a bit special and the query conditions will be constructed in the query generator
    ('date', None),
    ('daterange', None),
    )

def generate_queryselection_pairs():
    for (key, ormfield) in queryselection_pair_source:
        if isinstance(key, tuple):
            fieldname, value = key
        else:
            fieldname = key
            value = True

        # pytest doesn't like us calling a fixture, so making an alternate method
        q = build_query(None)
        if key == 'date':
            value = '20140506'
            # This works for UTC and Hawaii, at least
            startdt = datetime(2014, 5, 6)
            q = q.filter(Header.ut_datetime >= startdt).filter(Header.ut_datetime < (startdt + ONEDAY_OFFSET))
        elif key == 'daterange':
            value = '20110101-20150101'
            startdt = datetime(2011, 1, 1)
            enddt = datetime(2015, 1, 2)
            q = q.filter(Header.ut_datetime >= startdt).filter(Header.ut_datetime < enddt)
        elif isinstance(ormfield, (AsBoolean, BooleanClauseList)):
            q = q.filter(ormfield)
        elif isinstance(ormfield, MethodType):
            q = q.filter(ormfield(value))
        else:
            q = q.filter(ormfield == value)

        yield {fieldname: value}, str(compiled_statement(q.statement))

@pytest.mark.parametrize("input,expected", generate_queryselection_pairs())
@pytest.mark.slow
def test_queryselection(query, input, expected):
    q = queryselection(query, input)
    print (q)
    assert str(compiled_statement(queryselection(query, input).statement)) == expected
