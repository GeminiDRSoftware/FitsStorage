import pytest

from fits_storage.orm.curation import duplicate_present, present_not_canonical, duplicate_canonicals
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.diskfilereport import DiskFileReport
from fits_storage.orm.file import File
import sqlalchemy.orm.exc as orm_exc
import fits_storage.fits_storage_config as fsc
from fits_storage.orm.footprint import Footprint
from fits_storage.orm.fulltextheader import FullTextHeader
from fits_storage.orm.header import Header
from tests.file_helper import ensure_file


def _delete_diskfiles(session, filename):
    for df in session.query(DiskFile).filter(DiskFile.filename == filename).all():
        for fp in session.query(Footprint).join(Header, Footprint.header_id == Header.id) \
                .filter(Header.diskfile_id == df.id):
            session.delete(fp)
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
    assert(unpacked[0].filename == data_file)
    assert(unpacked[1].filename == data_file)


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
    assert(unpacked[0].filename == data_file)
    assert(unpacked[1].filename == data_file)


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
    assert(unpacked[0].filename == data_file)
