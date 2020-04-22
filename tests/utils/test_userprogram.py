import pytest
import sqlalchemy.orm.exc as orm_exc
from datetime import datetime, timedelta

from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.file import File
from fits_storage.orm.header import Header
from fits_storage.orm.miscfile import MiscFile
from fits_storage.orm.obslog import Obslog
from fits_storage.orm.user import User
from fits_storage.orm.userprogram import UserProgram
from fits_storage.utils.userprogram import canhave_header, canhave_obslog, canhave_miscfile
from fits_storage.utils.web import get_context
from tests.file_helper import ensure_file


def mock_populate_fits(self, diskfile, log):
    self.observation_id = 'testheader-0'


def mock_get_file_size(self):
    return 1


def mock_get_file_md5(self):
    return ''


def mock_get_lastmod(self):
    return datetime.now()


def get_or_create_user(session):
    try:
        user = session.query(User).filter(User.username == 'testuser').one()
    except orm_exc.NoResultFound:
        user = User('testuser')
        session.add(user)
        session.commit()
    return user


def get_or_create_userprogram(session, user):
    # We need to permission them for our obs id
    try:
        userprogram = session.query(UserProgram).filter(UserProgram.user_id == user.id) \
            .filter(UserProgram.observation_id == 'testheader-0').one()
    except orm_exc.NoResultFound:
        userprogram = UserProgram(user.id, observation_id='testheader-0')
        session.add(userprogram)
        session.commit()
    return userprogram


def get_or_create_file(session, filename):
    try:
        file = session.query(File).filter(File.name == filename).one()
    except orm_exc.NoResultFound:
        file = File(filename)
        session.add(file)
        session.commit()
    return file


def get_or_create_diskfile(session, file, filename):
    try:
        diskfile = session.query(DiskFile).filter(DiskFile.filename == filename) \
            .filter(DiskFile.canonical == True).one()
    except orm_exc.NoResultFound:
        diskfile = DiskFile(file, filename, '')
        diskfile.canonical = True
        session.add(diskfile)
        session.commit()
    return diskfile


def get_or_create_header(session, diskfile):
    try:
        header = session.query(Header).filter(Header.diskfile_id == diskfile.id).one()
    except orm_exc.NoResultFound:
        header = Header(diskfile)
        session.add(header)
        session.commit()
    return header


@pytest.mark.usefixtures("rollback")
class TestUserProgram:
    def test_canhave_header(self, session, monkeypatch):
        monkeypatch.setattr(Header, "populate_fits", mock_populate_fits)
        monkeypatch.setattr(DiskFile, "get_file_size", mock_get_file_size)
        monkeypatch.setattr(DiskFile, "get_file_md5", mock_get_file_md5)
        monkeypatch.setattr(DiskFile, "get_lastmod", mock_get_lastmod)

        user = get_or_create_user(session)
        userprogram = get_or_create_userprogram(session, user)
        file = get_or_create_file(session, 'testfile.fits')
        diskfile = get_or_create_diskfile(session, file, 'testfile.fits')
        header = get_or_create_header(session, diskfile)

        ctx = get_context(True)
        ctx.session = session

        assert canhave_header(session, user, header, filedownloadlog=None, gotmagic=False, user_progid_list=None,
                              user_obsid_list=None, user_file_list=None)


    def test_canhave_obslog(self, session, monkeypatch):
        monkeypatch.setattr(Header, "populate_fits", mock_populate_fits)
        monkeypatch.setattr(DiskFile, "get_file_size", mock_get_file_size)
        monkeypatch.setattr(DiskFile, "get_file_md5", mock_get_file_md5)
        monkeypatch.setattr(DiskFile, "get_lastmod", mock_get_lastmod)

        user = get_or_create_user(session)
        userprogram = get_or_create_userprogram(session, user)
        file = get_or_create_file(session, 'testfile.fits')
        diskfile = get_or_create_diskfile(session, file, 'testfile.fits')
        header = get_or_create_header(session, diskfile)

        # We need an obslog
        try:
            obslog = session.query(Obslog).filter(Obslog.diskfile_id == diskfile.id).one()
        except orm_exc.NoResultFound:
            obslog = Obslog(diskfile)
            obslog.date = datetime.now()
            session.add(obslog)
            session.commit()

        ctx = get_context(True)
        ctx.session = session

        assert canhave_obslog(session, user, obslog, filedownloadlog=None, gotmagic=False,
                              user_progid_list=None,
                              user_obsid_list=None, user_file_list=None)

    def test_canhave_miscfile(self, session, monkeypatch):
        monkeypatch.setattr(Header, "populate_fits", mock_populate_fits)
        monkeypatch.setattr(DiskFile, "get_file_size", mock_get_file_size)
        monkeypatch.setattr(DiskFile, "get_file_md5", mock_get_file_md5)
        monkeypatch.setattr(DiskFile, "get_lastmod", mock_get_lastmod)

        user = get_or_create_user(session)
        userprogram = get_or_create_userprogram(session, user)
        file = get_or_create_file(session, 'testfile.fits')
        diskfile = get_or_create_diskfile(session, file, 'testfile.fits')
        header = get_or_create_header(session, diskfile)

        # We need a miscfile
        try:
            misc = session.query(MiscFile).filter(MiscFile.diskfile_id == diskfile.id).one()
        except orm_exc.NoResultFound:
            misc = MiscFile()
            misc.diskfile = diskfile
            misc.release = datetime.now() + timedelta(days=500)
            session.add(misc)
            session.commit()

        ctx = get_context(True)
        ctx.session = session

        assert canhave_miscfile(session, user, misc, filedownloadlog=None,
                                gotmagic=False, user_progid_list=None,
                                user_obsid_list=None, user_file_list=None)

