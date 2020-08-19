import pytest

from fits_storage.orm.curation import duplicate_present, present_not_canonical, duplicate_canonicals
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.file import File


@pytest.mark.usefixtures("rollback")
def test_duplicate_canonicals(session):
    f = File("filename")
    session.add(f)
    df1 = DiskFile(f, "filename", "")
    df2 = DiskFile(f, "filename", "")
    session.add(df1)
    session.add(df2)
    diskfiles = duplicate_canonicals(session)
    assert(diskfiles is not None)
    unpacked = list(diskfiles)
    assert(len(unpacked) == 2)
    assert(unpacked[0].filename == "filename")
    assert(unpacked[1].filename == "filename")


@pytest.mark.usefixtures("rollback")
def test_duplicate_present(session):
    f = File("filename")
    session.add(f)
    df1 = DiskFile(f, "filename", "")
    df2 = DiskFile(f, "filename", "")
    df1.present = True
    df2.present = True
    session.add(df1)
    session.add(df2)
    diskfiles = duplicate_present(session)
    assert(diskfiles is not None)
    unpacked = list(diskfiles)
    assert(len(unpacked) == 2)
    assert(unpacked[0].filename == "filename")
    assert(unpacked[1].filename == "filename")


@pytest.mark.usefixtures("rollback")
def test_present_not_canonical(session):
    f = File("filename")
    session.add(f)
    df = DiskFile(f, "filename", "")
    df.present = True
    df.canonical = False
    session.add(df)
    diskfiles = present_not_canonical(session)
    assert(diskfiles is not None)
    unpacked = list(diskfiles)
    assert(len(unpacked) == 1)
    assert(unpacked[0].filename == "filename")
