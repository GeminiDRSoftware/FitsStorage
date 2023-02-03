from sqlalchemy import Column
from sqlalchemy import Integer, Text
from sqlalchemy.orm import relationship

from gemini_obs_db.orm import Base


__all__ = ["File"]


class File(Base):
    """
    This is the ORM class for the file table. This is highest level most abstract
    concept of a 'File'. It's essentially just a unique label that other things
    - actual DiskFiles for example can reference. The 'name' column here may not
    be the actual filename - the definitive filename is in the diskfile table,
    when we have a compressed (bzip2) file, we trim off the .bz2 here.

    Parameters
    ----------
    filename : str
        Name of the file for the record
    """
    __tablename__ = 'file'

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False, unique=True, index=True)

    diskfiles = relationship('DiskFile', lazy='dynamic', back_populates='file')

    def __init__(self, filename: str):
        """
        Create a :class:`~File` record from the filename

        Parameters
        ----------
        filename : str
            Name of the file for the record
        """
        self.name = File.trim_name(filename)

    def __repr__(self):
        """
        Get a string representation of the :class:`~gemini_obs_db.orm.file.File` object

        Returns
        -------
        str : string representation of the :class:`~gemini_obs_db.orm.file.File`
        """
        return "<File('%s', '%s')>" % (self.id, self.name)

    @staticmethod
    def trim_name(filename: str):
        """
        Trim any trailing .bz2 off the filename

        Parameters
        ----------
        filename : str
            Filename to trim

        Returns
        -------
            str : filename, with a trailing `.bz2` removed, if present
        """

        if filename.endswith(".bz2"):
            return filename[:-4]

        return filename
