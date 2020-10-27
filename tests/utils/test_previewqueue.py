import os

import pytest

from fits_storage import fits_storage_config
from fits_storage.fits_storage_config import preview_path, storage_root
from fits_storage.orm import sessionfactory
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.file import File
from fits_storage.orm.preview import Preview
from fits_storage.utils.previewqueue import PreviewQueueUtil
from fits_storage.utils.null_logger import EmptyLogger
from fits_storage.utils import queue

from tests.file_helper import ensure_file


def mock_pop_queue(queue_class, session, logger, fast_rebuild=False):
    return "test"


def mock_queue_length(queue_class, session):
    return 1


def test_preview_queue_util(monkeypatch):
    monkeypatch.setattr(queue, "pop_queue", mock_pop_queue)
    session = None
    logging = EmptyLogger()
    pqu = PreviewQueueUtil(session, logging)
    next = pqu.pop()
    assert(next == "test")


def test_preview_queue_length(monkeypatch):
    monkeypatch.setattr(queue, "queue_length", mock_queue_length)
    session = None
    logging = EmptyLogger()
    pqu = PreviewQueueUtil(session, logging)
    length = pqu.length()
    assert(length == 1)


@pytest.mark.usefixtures("rollback")
def test_process(session):
    logging = EmptyLogger()
    pqu = PreviewQueueUtil(session, logging)
    diskfiles = list()
    filename = "N20191010S0144.fits"
    path = ""

    ensure_file(filename)

    file = File(filename)
    session.add(file)
    session.flush()
    df = DiskFile(file, filename, path)
    session.add(df)
    session.flush()
    diskfiles.append(df)
    pqu.process(diskfiles, True)


@pytest.mark.usefixtures("rollback")
def test_reprocess(session):
    logging = EmptyLogger()
    pqu = PreviewQueueUtil(session, logging)
    diskfiles = list()
    filename = "N20191010S0144.fits"
    path = ""

    ensure_file(filename)

    # cleanup first
    file = session.query(File).filter(File.name == filename).one()
    if file is None:
        file = File(filename)
        session.add(file)
        session.flush()
    df = session.query(DiskFile).filter(DiskFile.file_id == file.id).one()
    if df is None:
        df = DiskFile(file, filename, path)
        session.add(df)
        session.flush()
    diskfiles.append(df)

    session.query(Preview).filter(Preview.diskfile_id == df.id).delete()
    session.flush()

    pqu.process(diskfiles, True, force=True)
    p = session.query(Preview).filter(Preview.diskfile_id == df.id).one()
    assert(p is not None)
    preview_full_path = os.path.join(storage_root, preview_path, 'N20191010S0144.fits_preview.jpg')
    access_time1 = os.path.getmtime(preview_full_path)
    pqu.process(diskfiles, True, force=True)
    p = session.query(Preview).filter(Preview.diskfile_id == df.id).one()
    assert(p is not None)
    access_time2 = os.path.getmtime(preview_full_path)
    # make sure we regenerated the preview
    assert(access_time1 != access_time2)

    session.rollback()
