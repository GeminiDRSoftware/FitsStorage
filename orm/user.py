from sqlalchemy import Column
from sqlalchemy import Integer, Text, Boolean, DateTime

from . import Base

from hashlib import sha256
from os import urandom
import datetime
from base64 import b32encode, standard_b64encode

# Note, password hashing follows the scheme at
# https://crackstation.net/hashing-security.htm

class User(Base):
    """
    This is the ORM class for the user table.
    """
    # Calling the table user makes it awkward in raw sql as that's a reserved name and you have to quote it
    __tablename__ = 'archiveuser'

    id = Column(Integer, primary_key=True)
    username = Column(Text, nullable=False, index=True)
    fullname = Column(Text)
    password = Column(Text)
    salt = Column(Text)
    email = Column(Text)
    gemini_staff = Column(Boolean)
    reset_token = Column(Text)
    reset_token_expires = Column(DateTime)
    cookie = Column(Text, index=True)
    account_created = Column(DateTime)
    password_changed = Column(DateTime)


    def __init__(self, username):
        self.username = username
        self.password = None
        self.gemini_staff = False
        self.reset_token = None
        self.cookie = None
        self.account_created = datetime.datetime.now()

    def reset_password(self, password):
        """
        Takes an actual password string, generates a random salt, hashes the password with the salt,
        updates the ORM with the new hash and the new salt.
        This function also expires any existing reset_token and session cookie
        Calling code needs to call a session.commit() after calling this.
        """
        hashobj = sha256()
        self.salt = standard_b64encode(urandom(256))
        hashobj.update(self.salt)
        hashobj.update(password)
        self.password = hashobj.hexdigest()
        password = None
        hashobj = None
        self.reset_token = None
        self.reset_token_expires = None
        self.cookie = None

    def validate_password(self, candidate):
        """
        Takes a candidate password string and checks if it's correct for this user
        Returns True if it is correct
        Returns False for wrong password
        """
        hashobj = sha256()
        hashobj.update(self.salt)
        hashobj.update(candidate)
        if (hashobj.hexdigest() == self.password):
            return True
        else:
            return False

    def generate_reset_token(self):
        """
        Generates a random password reset token, and sets an exiry date on it.
        The token can be emailed to the user in a password reset link,
        and then checked for validity when they click the link.
        Don't forget to commit the session after calling this.
        Returns the token for convenience
        """
        self.reset_token = b32encode(urandom(32))
        self.reset_token_expires = datetime.datetime.now() + datetime.timedelta(minutes=15)
        return self.reset_token

    def validate_reset_token(self, candidate):
        """
        Takes a candidate reset token and validates it for this user.
        If the token is valid, return True after nulling the token in the db.
        Returns False if the token is not valid or has expired
        Don't forget to commit the session after calling this so that a
        sucessfull validation will null the token making it one-time-use
        """
        if((self.reset_token is None) or (self.reset_token_expires is None)):
            return False
        if ((datetime.datetime.now() < self.reset_token_expires) and (candidate == self.reset_token)):
            self.reset_token = None
            self.reset_token_expires = None
            return True
        else:
            return False

    def generate_cookie(self):
        """
        Generates a random session cookie string for this user.
        Don't forget to commit the session after calling this
        """
        self.cookie = standard_b64encode(urandom(256))

    def log_in(self):
        """
        Call this when a user sucesfully logs in.
        Returns the session cookie
        Don't forget to commit the session afterwards
        """
        # Void any outstanding password reset tokens
        self.reset_token = None
        self.reset_token_expires = None
        # Generate a new session cookie only if one doesn't exist
        # (don't want to expire existing sessions just becaue we logged in from a new machine)
        if(self.cookie is None):
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
