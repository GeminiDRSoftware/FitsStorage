import pytest

from gemini_obs_db.calcache import CalCache
from fits_storage.orm.curation import duplicate_present, present_not_canonical, duplicate_canonicals
from gemini_obs_db.diskfile import DiskFile
from fits_storage.orm.diskfilereport import DiskFileReport
from gemini_obs_db.f2 import F2
from gemini_obs_db.file import File
import sqlalchemy.orm.exc as orm_exc
import fits_storage.fits_storage_config as fsc
from fits_storage.orm.footprint import Footprint
from fits_storage.orm.fulltextheader import FullTextHeader
from gemini_obs_db.gmos import Gmos
from gemini_obs_db.gnirs import Gnirs
from gemini_obs_db.gpi import Gpi
from gemini_obs_db.gsaoi import Gsaoi
from gemini_obs_db.header import Header
from gemini_obs_db.michelle import Michelle
from gemini_obs_db.nici import Nici
from gemini_obs_db.nifs import Nifs
from gemini_obs_db.niri import Niri
from tests.file_helper import ensure_file


def _delete_via_header(session, df, clazz):
    for fp in session.query(clazz).join(Header, clazz.header_id == Header.id) \
            .filter(Header.diskfile_id == df.id):
        session.delete(fp)


def _delete_diskfiles(session, filename):
    for df in session.query(DiskFile).filter(DiskFile.filename == filename).all():
        _delete_via_header(session, df, Footprint)
        _delete_via_header(session, df, Nici)
        _delete_via_header(session, df, F2)
        _delete_via_header(session, df, Gmos)
        _delete_via_header(session, df, Gnirs)
        _delete_via_header(session, df, Gpi)
        _delete_via_header(session, df, Gsaoi)
        _delete_via_header(session, df, Michelle)
        _delete_via_header(session, df, Nifs)
        _delete_via_header(session, df, Niri)
        for cc in session.query(CalCache).join(Header, CalCache.obs_hid == Header.id) \
                .filter(Header.diskfile_id == df.id):
            session.delete(cc)
        for cc in session.query(CalCache).join(Header, CalCache.cal_hid == Header.id) \
                .filter(Header.diskfile_id == df.id):
            session.delete(cc)
        session.query(Header).filter(Header.diskfile_id == df.id).delete()
        session.query(DiskFileReport).filter(DiskFileReport.diskfile_id == df.id).delete()
        session.query(FullTextHeader).filter(FullTextHeader.diskfile_id == df.id).delete()
        session.delete(df)


@pytest.mark.usefixtures("rollback")
def test_duplicate_canonicals(monkeypatch, session):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    data_file = 'S20130124S0036.fits'

    try:
        f = session.query(File).filter(File.name == data_file).one()
    except orm_exc.NoResultFound:
        f = File(data_file)
        session.add(f)
    _delete_diskfiles(session, data_file)

    ensure_file(data_file, '/tmp')

    df1 = DiskFile(f, data_file, "")
    df2 = DiskFile(f, data_file, "")
    session.add(df1)
    session.add(df2)
    diskfiles = duplicate_canonicals(session)
    assert(diskfiles is not None)
    unpacked = list(diskfiles)
    assert(len(unpacked) == 2)
    assert(unpacked[0][1].name == data_file)
    assert(unpacked[1][1].name == data_file)


@pytest.mark.usefixtures("rollback")
def test_duplicate_present(monkeypatch, session):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    data_file = 'S20130124S0036.fits'

    try:
        f = session.query(File).filter(File.name == data_file).one()
    except orm_exc.NoResultFound:
        f = File(data_file)
        session.add(f)
    _delete_diskfiles(session, data_file)

    df1 = DiskFile(f, data_file, "")
    df2 = DiskFile(f, data_file, "")
    df1.present = True
    df2.present = True
    session.add(df1)
    session.add(df2)
    diskfiles = duplicate_present(session)
    assert(diskfiles is not None)
    unpacked = list(diskfiles)
    assert(len(unpacked) == 2)
    assert(unpacked[0][1].name == data_file)
    assert(unpacked[1][1].name == data_file)


@pytest.mark.usefixtures("rollback")
def test_present_not_canonical(monkeypatch, session):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    data_file = 'S20130124S0036.fits'
    try:
        f = session.query(File).filter(File.name == data_file).one()
    except orm_exc.NoResultFound:
        f = File(data_file)
        session.add(f)
    _delete_diskfiles(session, data_file)

    df = DiskFile(f, data_file, "")
    df.present = True
    df.canonical = False
    session.add(df)
    diskfiles = present_not_canonical(session)
    assert(diskfiles is not None)
    unpacked = list(diskfiles)
    assert(len(unpacked) == 1)
    assert(unpacked[0][1].name == data_file)
