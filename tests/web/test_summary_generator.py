import collections

from datetime import datetime

import pytest

from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.file import File
from fits_storage.orm.header import Header
from fits_storage.web.summary_generator import formdata_to_compressed, selection_to_form_indices, \
    selection_to_column_names, ColDef, ColWrapper, Row, SummaryGenerator

# search_col_mapping = {
# #   col_key    (column_name, compressed_name)
#     'col_cls': ('observation_class', 'C'),
#     'col_typ': ('observation_type', 'T'),
#     'col_obj': ('object', 'O'),
#     'col_wvb': ('waveband', 'W'),
#     'col_exp': ('exposure_time', 'E'),
#     'col_air': ('airmass', 'A'),
#     'col_flt': ('filter_name', 'F'),
#     'col_fpm': ('focal_plane_mask', 'M'),
#     'col_bin': ('detector_binning', 'B'),
#     'col_cwl': ('central_wavelength', 'L'),
#     'col_dis': ('disperser', 'D'),
#     'col_ra' : ('ra', 'r'),
#     'col_dec': ('dec', 'd'),
#     'col_qas': ('qa_state', 'Q'),
#     'col_riq': ('raw_iq', 'i'),
#     'col_rcc': ('raw_cc', 'c'),
#     'col_rwv': ('raw_wv', 'w'),
#     'col_rbg': ('raw_bg', 'b'),
# }
from tests.file_helper import setup_mock_file_stuff


def test_formdata_to_compressed():
    check = formdata_to_compressed(['col_cls', 'col_typ'])
    assert(check == 'CT')


def test_selection_to_form_indices():
    check = selection_to_form_indices({'cols': ['r', 'd']})
    assert(check is not None)
    assert(len(check) == 2)
    assert(check[0] == 'col_ra')
    assert(check[1] == 'col_dec')


def test_selection_to_column_names():
    check = selection_to_column_names({'cols': ['r', 'd']})
    assert(check is not None)
    assert(len(check) == 2)
    assert(check[0] == 'ra')
    assert(check[1] == 'dec')


def test_col_wrapper_cons():
    col_def = ColDef(heading='heading',
                     longheading='longheading',
                     sortarrows=True,
                     want=True,
                     header_attr='header_attr',
                     diskfile_attr='diskfile_attr',
                     summary_func='summary_func')
    MockSummary = collections.namedtuple('MockSummary', 'links')
    summary = MockSummary(links=0x1)  # enable arrows
    cw = ColWrapper(summary, 'key', col_def)
    assert(cw.sortarrow)

    summary = MockSummary(links=0x0)  # disable arrows
    cw = ColWrapper(summary, 'key', col_def)
    assert(cw.sortarrow is False)

    summary = MockSummary(links=0x1)  # disable arrows via coldef
    col_def = ColDef(heading='heading',
                     longheading='longheading',
                     sortarrows=False,
                     want=True,
                     header_attr='header_attr',
                     diskfile_attr='diskfile_attr',
                     summary_func='summary_func')
    cw = ColWrapper(summary, 'key', col_def)
    assert(cw.sortarrow is False)

    # check attr
    assert(cw.heading == 'heading')

    # check str
    assert(str(cw) == "<ColWrapper 'key'>")


def test_row():
    r = Row()
    r.add('col1')
    r.add('col2')
    assert(r.columns == ['col1', 'col2'])
    assert(r.can_download is False)


@pytest.mark.usefixtures("rollback")
def test_summary_generator(session, monkeypatch):
    # so we can make on the fly DiskFiles etc later
    setup_mock_file_stuff(monkeypatch)

    sg = SummaryGenerator('summary', links=0xFF, uri=None, user=None, user_progid_list=['program', ],
                          user_obsid_list=None, user_file_list=None, additional_columns=())

    th = sg.table_header()

    # make a dummy file for testing the row
    f = File('foo.fits')
    session.add(f)
    session.flush()
    df = DiskFile(f, 'foo.fits', '')
    session.add(df)
    session.flush()
    h = Header(df, None)
    h.ut_datetime = datetime(2020, 5, 1)
    h.program_id = 'program'
    session.add(h)
    session.flush()

    tr = sg.table_row(h, df, f, comment=None)

    fn = sg.filename(h, df, f)
    assert(sg.procsci(h) is None)

    dlabel = sg.datalabel(h, comment=None)

    utd = sg.ut_datetime(h)
    ins = sg.instrument(h)
    obsc = sg.observation_class(h)
    obst = sg.observation_type(h)
    et = sg.exposure_time(h)
    am = sg.airmass(h)
    ra = sg.ra(h)
    dec = sg.dec(h)
    lt = sg.local_time(h)
    obj = sg.object(h)
    wb = sg.waveband(h)
    dl = sg.download(h, df, f, None)

    assert(tr is not None)
    assert(tr.can_download is False)
    assert(tr.has_provenance is False)
    assert(tr.procsci is None)
    assert(tr.uri is None)

    assert(fn is not None)
    assert(fn['links'] is True)
    assert(fn['name'] == 'foo.fits')
    assert(fn['df_id'] == df.id)

    assert(dlabel is not None)
    assert(dlabel['links'] is True)
    assert(dlabel['datalabel'] == 'None')  # could set this properly above...
    assert(dlabel['comment'] is None)
    assert(dlabel['display_prog'] is True)
    assert(dlabel['dl'] is not None)
    assert(dlabel['dl'].datalabel is None)

    assert(h is not None)
    assert(h.diskfile_id == df.id)

    assert(dl is not None)
    assert(dl['name'] == 'foo.fits')
    assert(dl['downloadable'] is True)

    assert(utd is not None)
    assert(utd['links'] is True)
    assert(utd['dp'] == '2020-05-01')
    assert(utd['tp'] == '00:00:00')
    assert(utd['dl'] == '20200501')

    assert(ins is not None)
    assert(ins['links'] is True)
    assert(ins['inst'] is None)
    assert(ins['ao'] is None)
    assert(ins['lg'] is None)

    assert(obsc is not None)
    assert(obsc['links'] is False)
    assert(obsc['text'] is None)

    assert(obst is not None)
    assert(obst['links'] is False)
    assert(obst['text'] is None)

    assert (et is not None)
    assert (et == '')

    assert(am is not None)
    assert(am == '')

    assert(ra is not None)
    assert(ra == '')

    assert(dec is not None)
    assert(dec == '')

    assert(lt == '')

    assert(obj is not None)
    assert(obj['links'] is True)
    assert(obj['name'] == 'None')  # ? wonder where that comes from

    assert(wb is None)  # could track down something that will set this

    session.rollback()
