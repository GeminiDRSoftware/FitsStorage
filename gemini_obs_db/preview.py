from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text

from . import Base


__all__ = ["Preview"]

from .diskfile import DiskFile


class Preview(Base):
    """
    This is the ORM object for the preview table. Use this to find preview (jpeg)
    files for a given diskfile.

    """
    __tablename__ = 'preview'

    id = Column(Integer, primary_key=True)
    diskfile_id = Column(Integer, ForeignKey('diskfile.id'), nullable=False,
                         index=True)
    filename = Column(Text)

    def __init__(self, diskfile: DiskFile, preview_filename: str):
        """
        Create a :class:`~Preview` record for the given :class:`~DiskFile` and associated preview filename

        Parameters
        ----------
        diskfile : :class:`~gemini_obs_db.diskfile.DiskFile`
            DiskFile record to store preview for
        preview_filename : str
            Filename of the preview file
        """
        self.diskfile_id = diskfile.id
        self.filename = preview_filename
