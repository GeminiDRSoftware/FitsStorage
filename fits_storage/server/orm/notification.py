from sqlalchemy import Column
from sqlalchemy import Integer, Text, Boolean

from fits_storage.core.orm import Base


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
        Create a notification with the given label

        Parameters
        ----------
        label : str
            Label to use for :class:`~Notification`
        """
        self.label = label

    @property
    def emailto(self):
        return self.piemail

    @property
    def emailcc(self):
        if self.ngoemail is None and self.csemail is None:
            return None
        elif self.ngoemail is None:
            return str(self.csemail)
        elif self.csemail is None:
            return str(self.ngoemail)
        else:
            return ', '.join([str(self.ngoemail), str(self.csemail)])
