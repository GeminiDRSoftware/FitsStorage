from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text

from . import Base


# ------------------------------------------------------------------------------
class UserFilePermission(Base):
    """
    This is the ORM class for the userfilepermission table. This provides the association
    for which users are associated with which files, and should have access to
    proprietary data from them.  See also userprogram.

    This is a N:M mapping.

    """
    __tablename__ = 'userfilepermission'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('archiveuser.id'), nullable=False, index=True)
    path = Column(Text, nullable=False)
    filename = Column(Text, nullable=False, index=True)

    def __init__(self, user_id, path, filename):
        self.user_id = user_id
        self.path = path
        self.filename = filename
