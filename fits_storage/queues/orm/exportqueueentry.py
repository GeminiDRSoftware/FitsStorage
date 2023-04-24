import datetime
from sqlalchemy import Column, UniqueConstraint
from sqlalchemy import Integer, Boolean, Text, DateTime

from fits_storage.core.orm import Base
from .ormqueuemixin import OrmQueueMixin


class ExportQueueEntry(OrmQueueMixin, Base):
    """
    This is the ORM object for the ExportQueue table.

    """
    __tablename__ = 'exportqueue'
    __table_args__ = (
        UniqueConstraint('filename', 'inprogress', 'fail_dt', 'destination'),
    )

    id = Column(Integer, primary_key=True)
    filename = Column(Text, nullable=False, index=True)
    path = Column(Text)
    destination = Column(Text, nullable=False, index=True)
    inprogress = Column(Boolean, index=True)
    fail_dt = Column(DateTime, index=True)
    sortkey = Column(Text, index=True)
    added = Column(DateTime)
    after = Column(DateTime)
    error = Column(Text)
    md5_before_header_update = Column(Text)
    md5_after_header_update = Column(Text)
    header_update = Column(Text)

    def __init__(self, filename, path, destination, after=None,
                 md5_before_header_update=None, md5_after_header_update=None,
                 header_update=None):
        """
        Create an :class:`~ExportQueueEntry` record

        Parameters
        ----------
        filename : str
            Name of the file to export
        path : str
            Path of the file within the `storage_root`
        destination : str
            URL of the server to export to
        after : datetime.datetime
            datetime only after which to export the file
        header_update: dict
            Header updates from a call to the update_headers API that resulted
            in this request for export. This gets passed from the ingest queue
            as a hint to try and avoid retransmitting the whole file
        md5_after_header_update: str
            md5sum of the file after the header update
        md5_before_header_update: str
            md5sum of the file before the header update
        """

        self.filename = filename
        self.sortkey = self.sortkey_from_filename()
        self.path = path
        self.destination = destination
        self.added = datetime.datetime.utcnow()
        self.after = after if after is not None else self.added
        self.inprogress = False
        self.fail_dt = self.fail_dt_false  # See Note in OrmQueueMixin
        self.header_update = header_update
        self.md5_before_header_update = md5_before_header_update
        self.md5_after_header_update = md5_after_header_update

        # TODO: A more refined way to prioritize archive exports
        prepend = 'z' if 'archive' in destination else 'a'
        self.sortkey = prepend + self.sortkey

    def __repr__(self):
        """
        Make a string representation of the queue item.

        Returns
        -------
        str : String representation of this record
        """
        return f"<ExportQueueEntry({self.id}, {self.filename}, " \
               f"{self.destination})>"
