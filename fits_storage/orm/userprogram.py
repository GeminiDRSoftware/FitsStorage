#
#                                                                    FitsStorage
#
#                                                             Gemini Observatory
#                                                  fits_store.orm.userprogram.py
# ------------------------------------------------------------------------------
__version__      = '0.99 beta'
# ------------------------------------------------------------------------------
from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text

from . import Base

# ------------------------------------------------------------------------------
class UserProgram(Base):
    """
    This is the ORM class for the userprogram table. This provides the association
    for which users are associated with which programs, and should have access to
    proprietary data from them.

    This is a N:M mapping.

    """
    __tablename__ = 'userprogram'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('archiveuser.id'), nullable=False, index=True)
    program_id = Column(Text, nullable=False, index=True)

    def __init__(self, user_id, program_id):
        self.user_id = user_id
        self.program_id = program_id

