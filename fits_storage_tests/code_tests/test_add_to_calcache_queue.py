from fits_storage_tests.code_tests.helpers import get_test_config
get_test_config()

import datetime
import sys

import pytest
from sqlalchemy import insert, select

from fits_storage_tests.code_tests.helpers import make_empty_testing_db_env

from fits_storage.db import sessionfactory
from fits_storage import utcnow

from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header
from fits_storage.queues.orm.calcachequeueentry import CalCacheQueueEntry

from fits_storage.scripts.add_to_calcache_queue import main


def _add_file(session, filename, canonical=True, mdready=True,
              instrument='GMOS-N', engineering=False, days_ago=0):
    # Insert minimal File, DiskFile and Header rows directly, bypassing the
    # ORM constructors, which need a real FITS file to read headers from.
    file_id = session.execute(
        insert(File).values(name=filename)).inserted_primary_key[0]
    df_id = session.execute(
        insert(DiskFile).values(file_id=file_id, filename=filename, path='',
                                canonical=canonical, present=True,
                                mdready=mdready)).inserted_primary_key[0]
    hid = session.execute(
        insert(Header).values(
            diskfile_id=df_id, instrument=instrument,
            engineering=engineering,
            ut_datetime=utcnow() - datetime.timedelta(days=days_ago))
    ).inserted_primary_key[0]
    session.commit()
    return hid


def _queued(session):
    # Filenames currently on the calcache queue
    session.commit()
    return sorted(session.scalars(select(CalCacheQueueEntry.filename)).all())


def _run(monkeypatch, *args):
    monkeypatch.setattr(sys, 'argv', ['add_to_calcache_queue.py', *args])
    main()


@pytest.fixture
def session(tmp_path):
    make_empty_testing_db_env(tmp_path)
    session = sessionfactory()
    _add_file(session, 'N20200101S0001.fits')
    _add_file(session, 'N20200101S0002.fits', instrument='F2', days_ago=10)
    _add_file(session, 'N20200202S0001.fits', days_ago=100)
    _add_file(session, 'N20200202S0002.fits', canonical=False)
    _add_file(session, 'N20200202S0003.fits', mdready=False)
    _add_file(session, 'N20200202S0004.fits', engineering=True)
    yield session
    session.close()


def test_requires_selection(session, monkeypatch):
    with pytest.raises(SystemExit) as excinfo:
        _run(monkeypatch)
    assert excinfo.value.code == 1
    assert _queued(session) == []


def test_all(session, monkeypatch):
    # Excludes non-canonical, metadata-bad and engineering files by default
    _run(monkeypatch, '--all')
    assert _queued(session) == ['N20200101S0001.fits',
                                'N20200101S0002.fits',
                                'N20200202S0001.fits']


def test_ignore_mdbad(session, monkeypatch):
    _run(monkeypatch, '--all', '--ignore-mdbad')
    assert 'N20200202S0003.fits' in _queued(session)


def test_include_eng(session, monkeypatch):
    _run(monkeypatch, '--all', '--include-eng')
    assert 'N20200202S0004.fits' in _queued(session)


def test_file_pre(session, monkeypatch):
    _run(monkeypatch, '--file-pre', 'N20200101')
    assert _queued(session) == ['N20200101S0001.fits',
                                'N20200101S0002.fits']


def test_file_pre_autoescape(session, monkeypatch):
    # '_' is an SQL LIKE wildcard, it must be matched literally
    _run(monkeypatch, '--file-pre', 'N2020_101')
    assert _queued(session) == []


def test_lastdays(session, monkeypatch):
    _run(monkeypatch, '--lastdays', '30')
    assert _queued(session) == ['N20200101S0001.fits',
                                'N20200101S0002.fits']


def test_lastdays_zero(session, monkeypatch):
    # --lastdays 0 is a valid selection, not a missing one
    _run(monkeypatch, '--lastdays', '0')
    assert _queued(session) == []


def test_instrument(session, monkeypatch):
    _run(monkeypatch, '--all', '--instrument', 'F2')
    assert _queued(session) == ['N20200101S0002.fits']


def test_precheck_skips_queued(session, monkeypatch):
    # Running twice should not fail or duplicate entries
    _run(monkeypatch, '--all')
    _run(monkeypatch, '--all')
    assert _queued(session) == ['N20200101S0001.fits',
                                'N20200101S0002.fits',
                                'N20200202S0001.fits']


def test_no_precheck_bulk_add_fails(session, monkeypatch):
    # Without the precheck, a bulk add that includes an already queued
    # entry fails as a whole, and nothing new gets added
    _run(monkeypatch, '--file-pre', 'N20200101S0001')
    with pytest.raises(SystemExit) as excinfo:
        _run(monkeypatch, '--all', '--no-precheck')
    assert excinfo.value.code == 1
    assert _queued(session) == ['N20200101S0001.fits']


def test_no_precheck_no_bulk_add(session, monkeypatch):
    # With individual commits, the duplicate is skipped and the rest added
    _run(monkeypatch, '--file-pre', 'N20200101S0001')
    _run(monkeypatch, '--all', '--no-precheck', '--no-bulk-add')
    assert _queued(session) == ['N20200101S0001.fits',
                                'N20200101S0002.fits',
                                'N20200202S0001.fits']
