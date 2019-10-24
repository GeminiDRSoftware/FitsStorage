import pytest
import sqlalchemy.orm.exc as orm_exc

from fits_storage.orm.user import User

@pytest.fixture(scope='session')
def user(request, session):
    try:
        user = session.query(User).filter(User.username == 'testuser').one()
    except orm_exc.NoResultFound:
        user = User('testuser')
        session.add(user)
        session.commit()

    return user

@pytest.mark.usefixtures("rollback")
class TestUser:
    def test_reset_and_validate_password(self, session, user):
        assert user.password is None  # For a just created user, this should be null
        user.reset_password('foobar')
        assert user.validate_password('foobar') == True

    def test_generate_and_validate_reset_token(self, session, user):
        assert user.reset_token is None
        token = user.generate_reset_token()
        assert user.validate_reset_token('foo') == False
        assert user.validate_reset_token(None) == False
        assert user.validate_reset_token(token) == True
        assert (user.reset_token is None and user.reset_token_expires is None) == True

    def test_log_in(self, session, user):
        # Test that we start with an empty cookie
        assert user.cookie is None
        # Test that a new cookie has been generated after log in
        user.log_in()
        assert user.cookie is not None
        # Make sure that a second login keeps the existing cookie
        cookie = user.cookie
        user.log_in()
        assert user.cookie == cookie
