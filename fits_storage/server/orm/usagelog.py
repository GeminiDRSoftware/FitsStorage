import datetime

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime, BigInteger
from sqlalchemy.orm import relationship

from fits_storage.core.orm import Base
from .user import User

# ------------------------------------------------------------------------------
class UsageLog(Base):
    """
    This is the ORM class for the usage log table.
    The id column is a BigInteger except on sqlite where it's an Integer because
    sqlite doesn't handle autoincrement on BigInteger
    """
    __tablename__ = 'usagelog'

    id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    utdatetime = Column(DateTime(timezone=False), index=True)
    user_id = Column(Integer, ForeignKey(User.id), nullable=True, index=True)
    ip_address = Column(Text, index=True)
    user_agent = Column(Text)
    referer = Column(Text)
    method = Column(Text)
    uri = Column(Text)
    this = Column(Text, index=True)
    bytes = Column(BigInteger)
    status = Column(Integer, index=True)
    notes = Column(Text)

    user = relationship(User)

    def __init__(self, ctx):
        """
        Create an initial UsageLog instance from the information in the context.
        Populates initial fields but does not add the instance to the session.

        Parameters
        ----------
        ctx : :class:`~Context`
            The context for this web session
        """
        self.utdatetime = datetime.datetime.utcnow()

        # We pass None for some testing context where we just need to
        # instantiate the object with no real content.
        if ctx is not None:
            self.ip_address = ctx.req.env.remote_ip
            self.user_agent = ctx.req.env.user_agent
            self.referer = ctx.req.env.referrer
            self.method = ctx.req.env.method
            self.uri = ctx.req.env.unparsed_uri

    def set_finals(self, ctx):
        """
        Sets the "final" values in the log object from the information in the
        context.

        Call this right at the end, after the request is basically finished.

        Parameters
        ----------
        ctx : :class:`~Context`
            The context for this web session
        """
        self.bytes = ctx.resp.bytes_sent
        self.status = ctx.resp.status

    def add_note(self, note):
        """
        Add a note to this log entry.

        Parameters
        ----------
        note : str
            The note to add
        """

        if self.notes is None:
            self.notes = note
        else:
            self.notes += "\n" + note

    @property
    def status_string(self):
        """
        Returns a string with the http status and a human readable translation.

        Returns
        -------
        str : String help text corresponding to the status
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
        elif self.status == 499:
            html += " (CLIENT CLOSED REQUEST)"

        return html
