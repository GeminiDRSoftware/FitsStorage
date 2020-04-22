import pytest
import sqlalchemy.orm.exc as orm_exc
from datetime import datetime, timedelta

from fits_storage.orm.diskfile import DiskFile
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


@pytest.mark.usefixtures("rollback")
class TestUserProgram:
    def test_canhave_header(self, session, monkeypatch):
        monkeypatch.setattr(Header, "populate_fits", mock_populate_fits)

        # We need a user
        try:
            user = session.query(User).filter(User.username == 'testuser').one()
        except orm_exc.NoResultFound:
            user = User('testuser')
            session.add(user)
            session.commit()

        # We need to permission them for our obs id
        try:
            session.query(UserProgram).filter(UserProgram.user_id == user.id) \
                .filter(UserProgram.observation_id == 'testheader-0').one()
        except orm_exc.NoResultFound:
            userprogram = UserProgram(user.id, observation_id='testheader-0')
            session.add(userprogram)
            session.commit()

        # We need a diskfile
        try:
            diskfile = session.query(DiskFile).filter(DiskFile.filename == 'N20200214S1347.fits') \
                .filter(DiskFile.canonical == True).one()
        except orm_exc.NoResultFound:
            diskfile = DiskFile('N20200214S1347.fits')
            session.add(diskfile)
            session.commit()

        # We need a header for the obs id
        try:
            header = session.query(Header).filter(Header.diskfile_id == diskfile.id).one()
        except orm_exc.NoResultFound:
            header = Header(diskfile)
            header.observation_id = 'testheader-0'
            session.add(header)
            session.commit()

        ctx = get_context(True)
        ctx.session = session

        assert canhave_header(session, user, header, filedownloadlog=None, gotmagic=False, user_progid_list=None,
                              user_obsid_list=None, user_file_list=None)


    def test_canhave_obslog(self, session, monkeypatch):
        monkeypatch.setattr(Header, "populate_fits", mock_populate_fits)

        # We need a user
        try:
            user = session.query(User).filter(User.username == 'testuser').one()
        except orm_exc.NoResultFound:
            user = User('testuser')
            session.add(user)
            session.commit()

        # We need to permission them for our obs id
        try:
            session.query(UserProgram).filter(UserProgram.user_id == user.id) \
                .filter(UserProgram.observation_id == 'testheader-0').one()
        except orm_exc.NoResultFound:
            userprogram = UserProgram(user.id, observation_id='testheader-0')
            session.add(userprogram)
            session.commit()

        # We need a diskfile
        try:
            diskfile = session.query(DiskFile).filter(DiskFile.filename == 'N20200214S1347.fits') \
                .filter(DiskFile.canonical == True).one()
        except orm_exc.NoResultFound:
            diskfile = DiskFile('N20200214S1347.fits')
            session.add(diskfile)
            session.commit()

        # We need a header for the obs id
        try:
            header = session.query(Header).filter(Header.diskfile_id == diskfile.id).one()
        except orm_exc.NoResultFound:
            header = Header(diskfile)
            header.observation_id = 'testheader-0'
            session.add(header)
            session.commit()

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

        # We need a user
        try:
            user = session.query(User).filter(User.username == 'testuser').one()
        except orm_exc.NoResultFound:
            user = User('testuser')
            session.add(user)
            session.commit()

        # We need to permission them for our obs id
        try:
            session.query(UserProgram).filter(UserProgram.user_id == user.id) \
                .filter(UserProgram.observation_id == 'testheader-0').one()
        except orm_exc.NoResultFound:
            userprogram = UserProgram(user.id, observation_id='testheader-0')
            session.add(userprogram)
            session.commit()

        # We need a diskfile
        try:
            diskfile = session.query(DiskFile).filter(DiskFile.filename == 'N20200214S1347.fits') \
                .filter(DiskFile.canonical == True).one()
        except orm_exc.NoResultFound:
            diskfile = DiskFile('N20200214S1347.fits')
            session.add(diskfile)
            session.commit()

        # We need a header for the obs id
        try:
            header = session.query(Header).filter(Header.diskfile_id == diskfile.id).one()
        except orm_exc.NoResultFound:
            header = Header(diskfile)
            header.observation_id = 'testheader-0'
            session.add(header)
            session.commit()

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

