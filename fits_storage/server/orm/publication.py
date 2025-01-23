from sqlalchemy import Column, ForeignKey, Integer, Text, Boolean
from sqlalchemy.orm import relationship

from fits_storage.core.orm import Base


class Publication(Base):
    """
    This is the ORM class for storing publication details fetched from the
    librarian's database.

    """
    __tablename__ = 'publication'

    id = Column(Integer, primary_key=True)
    bibcode = Column(Text, unique=True, index=True)
    author = Column(Text)
    title = Column(Text)
    year = Column(Integer)
    journal = Column(Text)
    telescope = Column(Text)
    instrument = Column(Text)
    country = Column(Text)
    wavelength = Column(Text)
    mode = Column(Text)
    gstaff = Column(Boolean)
    gsa = Column(Boolean)
    golden = Column(Boolean)
    too = Column(Boolean)
    partner = Column(Text)

    programs = relationship('Program', secondary='programpublication',
                            back_populates='publications', collection_class=set)

    def __init__(self, bibcode):
        """
        Create a publication record with the given bibcode

        Parameters
        ----------
        bibcode : str
            Bibliography code for the publication
        """
        if isinstance(bibcode, str):
            self.bibcode = bibcode.strip()
        else:
            self.bibcode = bibcode


class ProgramPublication(Base):
    """
    Association object supporting the M:N relationship between programs and
    publications. Note, this references the Program table, which is populated
    from ODB data, so it's necessary for the *program* to have been fetched
    from the ODB *before* publications can be associated with it.

    """
    __tablename__ = 'programpublication'

    id = Column(Integer, primary_key=True)
    program_id = Column(Integer, ForeignKey('program.id'),
                        nullable=False, index=True)
    publication_id = Column(Integer, ForeignKey('publication.id'),
                            nullable=False, index=True)

    def __init__(self, program, publication):
        """
        Create a new association between a program and a publication.

        Parameters
        ----------
        program : :class:`~program.Program`
            Program to associate
        publication : :class:`~publication.Publication`
            Publication to associate
        """
        if program is None:
            self.program_id = None
        else:
            self.program_id = program.id
        self.publication_id = publication.id
