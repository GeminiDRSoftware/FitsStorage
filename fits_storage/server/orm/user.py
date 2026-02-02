from sqlalchemy import Column
from sqlalchemy import String, Integer, Text, Boolean, DateTime

from fits_storage.core.orm import Base
from fits_storage import utcnow

from hashlib import sha256
from os import urandom
from datetime import datetime, timedelta
from base64 import b32encode, standard_b64encode

# Note, password hashing follows the scheme at
# https://crackstation.net/hashing-security.htm


class User(Base):
    """
    This is the ORM class for the user table.

    """
    # Calling the table user would make it awkward in raw sql as that's a
    # reserved name, and thus you'd have to quote it each time.
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    orcid_id = Column(String(20), index=True)
    noirlab_id = Column(Text, index=True)
    username = Column(Text, nullable=False, index=True)
    fullname = Column(Text)
    password = Column(Text)
    salt = Column(Text)
    email = Column(Text)
    gemini_staff = Column(Boolean)
    misc_upload = Column(Boolean)
    user_admin = Column(Boolean)
    file_permission_admin = Column(Boolean)
    instrument_team = Column(Text)
    superuser = Column(Boolean)
    reset_token = Column(Text)
    reset_token_expires = Column(DateTime)
    cookie = Column(Text, index=True)
    account_created = Column(DateTime)
    password_changed = Column(DateTime)
    last_login = Column(DateTime)
    last_login_by = Column(Text)

    def __init__(self, username):
        """
        Create a User record with the given username

        Parameters
        ----------
        username : str
            Username for the account
        """
        self.account_type = None
        self.username = username
        self.password = None
        self.gemini_staff = False
        self.misc_upload = False
        self.user_admin = False
        self.superuser = False
        self.reset_token = None
        self.cookie = None
        self.account_created = utcnow()

    def _clear_reset_token(self):
        self.reset_token = None
        self.reset_token_expires = None

    def reset_password(self, password):
        """
        Calls change_password to set the password to the given string
        This function also expires any existing reset_token and session cookie
        Calling code needs to call a session.commit() after calling this.

        Parameters
        ----------
        password : str
            New value for the password (unhashed)
        """
        self.change_password(password)
        self._clear_reset_token()
        self.cookie = None

    def change_password(self, password):
        """
        Takes an actual password string, generates a random salt, hashes the 
        password with the salt, updates the ORM with the new hash and the new
        salt. Calling code needs to call a session.commit() after calling this.

        Parameters
        ----------
        password : str
            Password (unhashed) to be salted, hashed and saved to the database
        """
        hashobj = sha256()
        salt = standard_b64encode(urandom(256))
        self.salt = salt.decode('utf8')
        hashobj.update(salt)
        hashobj.update(password.encode('utf8'))
        self.password = hashobj.hexdigest()
        password = None
        hashobj = None
        self.password_changed = utcnow()

    def validate_password(self, candidate):
        """
        Checks if a candidate password string is correct for this user.

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
        Generates a random password reset token, and sets an expiry date on it.
        The token can be emailed to the user in a password reset link,
        and then checked for validity when they click the link.
        Don't forget to commit the session after calling this.
        Returns the token for convenience.

        Returns
        -------
        str : token to pass back in for a password reset
        """
        self.reset_token = b32encode(urandom(32)).decode('utf-8')
        self.reset_token_expires = utcnow() + timedelta(minutes=15)
        return self.reset_token

    def validate_reset_token(self, candidate):
        """
        Takes a candidate reset token and validates it for this user.
        If the token is valid, return True after nulling the token in the db.
        Returns False if the token is not valid or has expired
        Don't forget to commit the session after calling this so that a
        successful validation will null the token making it one-time-use

        Returns
        -------
        bool : True if candidate successfully passed in a matching reset token
        """
        # Is a reset token even defined for this user?
        if self.reset_token is None or self.reset_token_expires is None:
            return False

        if (candidate is not None) and \
                (utcnow() < self.reset_token_expires) and \
                (candidate == self.reset_token):
            self._clear_reset_token()
            return True

        return False

    def generate_cookie(self):
        """
        Generates a random session cookie string for this user.
        Don't forget to commit the session after calling this.

        """
        self.cookie = standard_b64encode(urandom(256)).decode('utf-8')

    def log_in(self, by=None):
        """
        Call this when a user successfully logs in.
        Returns the session cookie.
        Don't forget to commit the session afterwards.

        by is a text string describing how the user authenticated. In-use
        values are 'local_account', 'orcid', 'noirlab'

        Returns
        -------
        str : generated cookie value
        """
        # Void any outstanding password reset tokens
        self._clear_reset_token()
        # Generate a new session cookie only if one doesn't exist
        # (don't want to expire existing sessions just because we logged in from
        # a new machine)
        if self.cookie is None:
            self.generate_cookie()

        # Record last_login time and login method
        self.last_login = utcnow()
        self.last_login_by = by

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
        """
        Check if reset has been requested

        Returns
        -------
        bool : True if a reset token is set
        """
        return self.reset_token is not None

    @property
    def reset_active(self):
        """
        Check if a reset is active

        Returns
        -------
        bool : True if a reset has been requested and the token has not expired
        """
        return self.reset_requested and (self.reset_token_expires > utcnow())

    @property
    def has_password(self):
        """
        Check if a password is set

        Returns
        -------
        bool : True if a password has been set
        """
        return self.password is not None
