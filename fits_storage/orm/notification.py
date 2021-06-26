from sqlalchemy import Column
from sqlalchemy import Integer, Text, Boolean

from gemini_obs_db import Base

# ------------------------------------------------------------------------------
class Notification(Base):
    """
    This is the ORM class for the table holding the email notification list for
    this server.

    """
    __tablename__ = 'notification'

    id = Column(Integer, primary_key=True)
    label = Column(Text)
    selection = Column(Text)
    piemail = Column(Text)
    ngoemail = Column(Text)
    csemail = Column(Text)
    internal = Column(Boolean)

    def __init__(self, label):
        """
        Create a motification with the given label

        Parameters
        ----------
        label : str
            Label to use for :class:`~Notification`
        """
        self.label = label


