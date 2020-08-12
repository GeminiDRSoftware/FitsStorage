from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text

from . import Base


class UserProgram(Base):
    """
    This is the ORM class for the userprogram table. This provides the association
    for which users are associated with which programs, observations, or files and
    should have access to proprietary data from them.

    This is a N:M mapping.

    """
    __tablename__ = 'userprogram'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('archiveuser.id'), nullable=False, index=True)
    program_id = Column(Text, nullable=True, index=True)
    observation_id = Column(Text, nullable=True, index=True)
    filename = Column(Text, nullable=True, index=True)

    def __init__(self, user_id, program_id=None, observation_id=None, filename=None, path=None):
        """
        Create a UserProgram record linking a user and program, observation, or file together.
        These linked records encompass what proprietary data the user has access to.

        Parameters
        ----------
        user_id : str
            ID of the linked user
        program_id : str
            Program ID of the permitted program
        observation_id : str
            Observation ID of the permitted observation
        filename : str
            Filename of the file
        path : str
            Path of the file (works in conjunction with `filename`)
        """
        count = 0
        if program_id:
            count = count+1
        if observation_id:
            count = count+1
        if filename:
            count = count+1
        if count == 0:
            raise ValueError("Must specify program_id, observation_id or filename")
        if count > 1:
            raise ValueError("Must specify only one of program_id, observation_id and filename")
        if path and not filename:
            raise ValueError("Path specified without filename")
        self.user_id = user_id
        self.program_id = program_id
        self.observation_id = observation_id
        if filename and path is None:
            # let's do this so naive concats work later
            path = ""
        self.path = path
        self.filename = filename
