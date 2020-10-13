import sqlalchemy
from datetime import datetime

import collections

import pytest

import fits_storage
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.file import File
from fits_storage.orm.header import Header
from fits_storage.orm.miscfile import MiscFile
from fits_storage.orm.obslog import Obslog
from fits_storage.orm.usagelog import UsageLog
from fits_storage.web import fileserver
from fits_storage.web.fileserver import generate_filename, make_tarinfo, download_post, is_regular_file, is_obslog, \
    is_misc, download
from tests.file_helper import setup_mock_file_stuff
from tests.web_helper import MockContext

generate_filename_inputs = [
    (True, {}, 'gemini_calibs.tar'),
    (True, {'program_id': 'progid',
            'observation_id': 'obsid',
            'inst': 'inst',
            'date': 'dt',
            'daterange': 'dtrange',
            'obsclass': 'obsclass',
            'obstype': 'obstype'}, 'gemini_calibs.progid_obsid_inst_dt_dtrange_obsclass_obstype.tar'),
    (False, {}, 'gemini_data.tar'),
    (False, {'program_id': 'progid',
             'observation_id': 'obsid',
             'inst': 'inst',
             'date': 'dt',
             'daterange': 'dtrange',
             'obsclass': 'obsclass',
             'obstype': 'obstype'}, 'gemini_data.progid_obsid_inst_dt_dtrange_obsclass_obstype.tar'),
]


@pytest.mark.parametrize("cals,selection,expected", generate_filename_inputs)
def test_generate_filename(cals, selection, expected):
    assert generate_filename(cals, selection) == expected


def test_make_tarinfo():
    ti = make_tarinfo('tarinfo', uname='fitsdata', size=100)
    assert ti is not None
    assert ti.name == 'tarinfo'
    assert ti.uname == 'fitsdata'
    assert ti.size == 100


@pytest.mark.usefixtures("rollback")
def test_download_post(session, monkeypatch):
    saw_selection = None
    saw_associated_calibrations = None

    def download(selection, associated_calibrations):
        nonlocal saw_selection
        nonlocal saw_associated_calibrations
        saw_selection = selection
        saw_associated_calibrations = associated_calibrations

    Field = collections.namedtuple('Field', 'name value')
    mock_context = MockContext(session, method='GET', form_data={'files': [Field(name='file', value='filea.fits'),
                                                                           Field(name='file', value='fileb.fits')]})

    def _mock_get_context(initialize=True):
        return mock_context

    monkeypatch.setattr(fits_storage.web.fileserver, "get_context", _mock_get_context)
    monkeypatch.setattr(fileserver, 'download', download)

    download_post()

    assert(saw_selection == {'filelist': ['filea.fits', 'fileb.fits']})
    assert(saw_associated_calibrations == False)


@pytest.mark.usefixtures("rollback")
def test_is_regular_file(session, monkeypatch):
    setup_mock_file_stuff(monkeypatch)

    f = File('foo.fits')
    session.add(f)
    session.flush()
    df = DiskFile(f, 'foo.fits', '')
    session.add(df)
    session.flush()
    h = Header(df)
    session.add(h)
    session.flush()

    h2, content_type = is_regular_file(session, df)
    assert (h2 == h)
    assert (content_type == 'application/fits')

    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_is_obslog(session, monkeypatch):
    setup_mock_file_stuff(monkeypatch)

    f = File('foo.fits')
    session.add(f)
    session.flush()
    df = DiskFile(f, 'foo.fits', '')
    session.add(df)
    session.flush()
    ol = Obslog(df)
    session.add(ol)
    session.flush()
    h = Header(df)
    session.add(h)
    session.flush()

    ol2, content_type = is_obslog(session, df)
    assert (ol2 == ol)
    assert (content_type == 'text/plain')

    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_is_miscfile(session, monkeypatch):
    setup_mock_file_stuff(monkeypatch)

    f = File('foo.fits')
    session.add(f)
    session.flush()
    df = DiskFile(f, 'foo.fits', '')
    session.add(df)
    session.flush()
    mf = MiscFile()
    mf.diskfile_id = df.id
    mf.release = datetime.now()
    session.add(mf)
    session.flush()
    h = Header(df)
    session.add(h)
    session.flush()

    mf2, content_type = is_misc(session, df)
    assert (mf2 == mf)
    assert (content_type == 'application/octect-stream')

    session.rollback()


@pytest.mark.usefixtures("rollback")
def test_download(session, monkeypatch):
    monkeypatch.setattr(sqlalchemy.orm.session.Session, 'commit', sqlalchemy.orm.session.Session.flush)

    Field = collections.namedtuple('Field', 'name value')
    # mock_context = MockContext(session, method='GET', form_data={'files': [Field(name='file', value='filea.fits'),
    #                                                                        Field(name='file', value='fileb.fits')]})
    mock_context = MockContext(session, method='GET', usagelog=None)
    usagelog=UsageLog(mock_context)
    session.add(usagelog)
    session.flush()
    mock_context.usagelog=usagelog

    def _mock_get_context(initialize=True):
        return mock_context

    monkeypatch.setattr(fits_storage.web.fileserver, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.list_headers, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.utils.web.adapter, "get_context", _mock_get_context)

    setup_mock_file_stuff(monkeypatch)

    f = File('foo.fits')
    session.add(f)
    session.flush()
    df = DiskFile(f, 'foo.fits', '')
    df.canonical = True
    df.present = True
    session.add(df)
    session.flush()
    mf = MiscFile()
    mf.diskfile_id = df.id
    mf.release = datetime.now()
    session.add(mf)
    session.flush()
    h = Header(df)
    h.ut_datetime = datetime(2020, 1, 1, 12)
    session.add(h)
    session.flush()

    download({'dt': '20200101'}, False)

    assert(mock_context.resp.status == 200)

    session.rollback()

