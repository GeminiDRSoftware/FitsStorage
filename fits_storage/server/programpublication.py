from sqlalchemy import Column, UniqueConstraint
from sqlalchemy import Integer, Text, String
from sqlalchemy.orm import backref, relationship

from fits_storage.core import Base
from .program import Program
from .publication import Publication

# ------------------------------------------------------------------------------
class ProgramPublication(Base):
    """
    Association object supporting the M:N relationship between programs and
    publications.

    """
    __tablename__ = 'programpublication'

    id = Column(Integer, primary_key=True)
    prog_id = Column(Integer, nullable = True, index = True)
    pub_id = Column(Integer, nullable = False, index = True)
    program_text_id = Column(Text, nullable=False, index=True)
    bibcode = Column(String(20), nullable=False, index=True)
    program = relationship(Program, foreign_keys=prog_id,
                           primaryjoin="ProgramPublication.prog_id==Program.id",
                           backref=backref("program_publications"))
    publication = relationship(Publication, foreign_keys=pub_id,
                           primaryjoin="ProgramPublication.pub_id==Publication.id",
                           backref=backref("publication_programs"))

    __table_args__ = (
            UniqueConstraint('prog_id', 'pub_id', name='programpublication_unique'),
            )

    def __init__(self, program, publication, program_text_id=None):
        """
        Create a new link between a program and a publication.

        These records help us track which programs are associated with a publication.

        Parameters
        ----------
        program : :class:`~program.Program`
            Program to link to a publication
        publication : :class:`~publication.Publication`
            Publication to add a linked program to
        program_text_id : str
            Optional program ID (the text id)
        """
        if program is None:
            self.prog_id = None
            self.program_text_id = program_text_id
        else:
            self.prog_id = program.id
            self.program_text_id = program.program_id
        self.pub_id = publication.id
        self.bibcode = publication.bibcode
