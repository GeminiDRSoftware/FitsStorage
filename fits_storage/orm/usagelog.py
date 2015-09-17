import datetime

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime
from sqlalchemy.orm import relation

from ..fits_storage_config import using_apache
from ..apache_return_codes import REMOTE_NOLOOKUP

from . import Base
from .user import User

from . import sessionfactory

class UsageLog(Base):
    """
    This is the ORM class for the usage log table
    """
    __tablename__ = 'usagelog'

    id = Column(Integer, primary_key=True)
    utdatetime = Column(DateTime(timezone=False), index=True)
    user_id = Column(Integer, ForeignKey('archiveuser.id'), nullable=True, index=True)
    ip_address = Column(Text, index=True)
    user_agent = Column(Text)
    referer = Column(Text)
    method = Column(Text)
    uri = Column(Text)
    this = Column(Text, index=True)
    bytes = Column(Integer)
    status = Column(Integer, index=True)
    notes = Column(Text)

    user = relation(User)

    def __init__(self, req):
        """
        Create an initial UsageLog instance from a mod_python apache request object.
        Populates initial fields but does not add the instance to the session.
        """
        self.utdatetime = datetime.datetime.utcnow()
        if using_apache:
            self.ip_address = req.get_remote_host(REMOTE_NOLOOKUP)
            if 'User-Agent' in req.headers_in.keys():
                self.user_agent = req.headers_in['User-Agent']
            if 'Referer' in req.headers_in.keys():
                self.referer = req.headers_in['Referer']
        self.method = req.method
        self.uri = req.unparsed_uri

    def set_finals(self, req):
        """
        Sets the "final" values in the log object from the request object.
        Call this right at the end, after the request is basically finished
        """
        self.bytes = req.bytes_sent
        self.status = req.status

    def add_note(self, note):
        """
        Add a note to this log entry.
        """

        if self.notes is None:
            self.notes = note
        else:
            self.notes += "\n" + note

    @property
    def status_string(self):
        """
        Returns a string with the http status and a human readable translation
        """
        html = str(self.status)
        if self.status == 200:
            html += " (OK)"
        elif self.status == 303:
            html += " (REDIRECT)"
        elif self.status == 403:
            html += " (FORBIDDEN)"
        elif self.status == 500:
            html += " (SERVER ERROR)"
        elif self.status == 404:
            html += " (NOT FOUND)"
        elif self.status == 405:
            html += " (METHOD NOT ALLOWED)"
        elif self.status == 406:
            html += " (NOT ACCEPTABLE)"

        return html
