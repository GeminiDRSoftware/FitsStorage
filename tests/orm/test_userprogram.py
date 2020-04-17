import pytest
import sqlalchemy.orm.exc as orm_exc

from fits_storage.orm.filedownloadlog import FileDownloadLog
from fits_storage.orm.usagelog import UsageLog
from fits_storage.orm.user import User
from fits_storage.orm.userfilepermission import UserFilePermission
from fits_storage.utils.userprogram import is_user_file_permission
from fits_storage.utils.web import get_context


@pytest.mark.usefixtures("rollback")
class TestUserProgram:
    def test_is_user_file_permission(self, session):
        user = None
        path = "path"
        filename = "filename.fits"

        try:
            user = session.query(User).filter(User.username == 'testuser').one()
        except orm_exc.NoResultFound:
            user = User('testuser')
            session.add(user)
            session.commit()

        try:
            session.query(UserFilePermission).filter(UserFilePermission.user_id == user.id) \
                .filter(UserFilePermission.path == path).filter(UserFilePermission.filename == filename).one()
        except orm_exc.NoResultFound:
            userfileperm = UserFilePermission(user_id=user.id, path=path, filename=filename)
            session.add(userfileperm)
            session.commit()

        assert is_user_file_permission(session, user, path, filename, None)
        assert not is_user_file_permission(session, user, path, "otherfilename.fits", None)
