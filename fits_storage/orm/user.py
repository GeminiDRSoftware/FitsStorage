from sqlalchemy import Column
from sqlalchemy import Integer, Text, Boolean, DateTime

from . import Base

from hashlib import sha256
from os import urandom
from datetime import datetime, timedelta
from base64 import b32encode, standard_b64encode

# Note, password hashing follows the scheme at
# https://crackstation.net/hashing-security.htm

# ------------------------------------------------------------------------------
class User(Base):
    """
    This is the ORM class for the user table.

    """
    # Calling the table user makes it awkward in raw sql as that's a reserved name
    # and you have to quote it.
    __tablename__ = 'archiveuser'

    id = Column(Integer, primary_key=True)
    account_type = Column(Text)
    username = Column(Text, nullable=False, index=True)
    fullname = Column(Text)
    password = Column(Text)
    salt = Column(Text)
    email = Column(Text)
    gemini_staff = Column(Boolean)
    misc_upload = Column(Boolean)
    superuser = Column(Boolean)
    reset_token = Column(Text)
    reset_token_expires = Column(DateTime)
    cookie = Column(Text, index=True)
    account_created = Column(DateTime)
    password_changed = Column(DateTime)

    def __init__(self, username):
        self.account_type = None
        self.username = username
        self.password = None
        self.gemini_staff = False
        self.misc_upload = False
        self.superuser = False
        self.reset_token = None
        self.cookie = None
        self.account_created = datetime.utcnow()

    def _clear_reset_token(self):
        self.reset_token = None
        self.reset_token_expires = None

    def reset_password(self, password):
        """
        Calls change_password to set the password to the given string
        This function also expires any existing reset_token and session cookie
        Calling code needs to call a session.commit() after calling this.

        """
        self.change_password(password)
        self._clear_reset_token()
        self.cookie = None

    def change_password(self, password):
        """
        Takes an actual password string, generates a random salt, hashes the 
        password with the salt, updates the ORM with the new hash and the new salt.
        Calling code needs to call a session.commit() after calling this.

        """
        hashobj = sha256()
        salt = standard_b64encode(urandom(256))
        self.salt = salt.decode('utf8')
        hashobj.update(salt)
        hashobj.update(password.encode('utf8'))
        self.password = hashobj.hexdigest()
        password = None
        hashobj = None
        self.password_changed = datetime.utcnow()

    def validate_password(self, candidate):
        """
        Takes a candidate password string and checks if it's correct for this user.

        Parameters
        ----------
        candidate: <str>
            candidate password string

        Returns 
        -------
        <bool>  True if correct.
                False if incorrect.

        """
        # If password hasn't been set yet
        if self.salt is None or self.password is None:
            return False

        hashobj = sha256()
        hashobj.update(self.salt.encode('utf8'))
        hashobj.update(candidate.encode('utf8'))
        if hashobj.hexdigest() == self.password:
            return True
        else:
            return False

    def generate_reset_token(self):
        """
        Generates a random password reset token, and sets an exiry date on it.
        The token can be emailed to the user in a password reset link,
        and then checked for validity when they click the link.
        Don't forget to commit the session after calling this.
        Returns the token for convenience.

        """
        self.reset_token = b32encode(urandom(32)).decode('utf-8')
        self.reset_token_expires = datetime.utcnow() + timedelta(minutes=15)
        return self.reset_token

    def validate_reset_token(self, candidate):
        """
        Takes a candidate reset token and validates it for this user.
        If the token is valid, return True after nulling the token in the db.
        Returns False if the token is not valid or has expired
        Don't forget to commit the session after calling this so that a
        sucessfull validation will null the token making it one-time-use
        """
        try:
            if (candidate is not None) and (datetime.utcnow() < self.reset_token_expires) and (candidate == self.reset_token):
                self._clear_reset_token()
                return True
        except TypeError: # If this happens, selt.reset_token_expires was None
            pass

        return False

    def generate_cookie(self):
        """
        Generates a random session cookie string for this user.
        Don't forget to commit the session after calling this.

        """
        self.cookie = standard_b64encode(urandom(256)).decode('utf-8')

    def log_in(self):
        """
        Call this when a user sucesfully logs in.
        Returns the session cookie.
        Don't forget to commit the session afterwards.

        """
        # Void any outstanding password reset tokens
        self._clear_reset_token()
        # Generate a new session cookie only if one doesn't exist
        # (don't want to expire existing sessions just becaue we logged in from a
        # new machine)
        if self.cookie is None:
            self.generate_cookie()
        return self.cookie

    def log_out_all(self):
        """
        Call this function and commit the session to log out all instances
        of this user by nulling their cookie. Next time they sucessfully log in,
        a new cookie will be generated for them.

        """
        self.cookie = None
        return self.cookie

    @property
    def reset_requested(self):
        return self.reset_token is not None

    @property
    def reset_active(self):
        return self.reset_requested and (self.reset_token_expires > datetime.utcnow())

    @property
    def has_password(self):
        return self.password is not None

    @property
    def is_staffer(self):
        return self.gemini_staff
