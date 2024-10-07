from sqlalchemy import Column
from sqlalchemy import Integer, Text
from sqlalchemy.orm import relationship

from fits_storage.core.orm import Base


class Program(Base):
    """
    This is the ORM class for storing program details fetched from the ODB.

    """
    __tablename__ = 'program'

    id = Column(Integer, primary_key=True)
    program_id = Column(Text, unique=True, index=True)
    pi_coi_names = Column(Text, index=True)
    title = Column(Text)
    abstract = Column(Text)

    publications = relationship('Publication', secondary='programpublication',
                                back_populates='programs', collection_class=set)

    def __init__(self, progdict):
        """
        Create a new Program instance, using values from the supplied
        program dictionary
        """

        self.program_id = progdict.get('id')
        self.update(progdict)

    def update(self, progdict):
        """
        Update a program, or populate fields in a new program, using values
        from the supplied dictionary. This updates all fields apart from the
        program ID.

        Parameters
        ----------
        progdict : dict
            Dictionary containing values to populate
        """
        self.pi_coi_names = progdict.get('investigator_names')
        self.title = progdict.get('title')
        self.abstract = progdict.get('abstract')
