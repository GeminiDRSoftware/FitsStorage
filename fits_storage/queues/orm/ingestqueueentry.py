"""
This module contains the IngestQueueEntry ORM class.

"""
import datetime

from sqlalchemy import Column, UniqueConstraint
from sqlalchemy import Integer, Boolean, Text, DateTime

from fits_storage.core.orm import Base
from fits_storage import utcnow
from .ormqueuemixin import OrmQueueMixin

from fits_storage.config import get_config


class IngestQueueEntry(OrmQueueMixin, Base):
    """
    This is the ORM object for the IngestQueue table
    """
    __tablename__ = 'ingestqueue'
    __table_args__ = (
        UniqueConstraint('filename', 'path', 'inprogress', 'fail_dt'),
    )

    id = Column(Integer, primary_key=True)
    filename = Column(Text, nullable=False, index=True)
    path = Column(Text, nullable=False)
    inprogress = Column(Boolean, index=True)
    fail_dt = Column(DateTime, index=True)
    added = Column(DateTime)
    force_md5 = Column(Boolean)
    force = Column(Boolean)
    after = Column(DateTime)
    no_defer = Column(Boolean)
    batch = Column(Text)
    sortkey = Column(Text, index=True)
    error = Column(Text)
    md5_before_header_update = Column(Text)
    md5_after_header_update = Column(Text)
    header_update = Column(Text)

    # Store these configuration items in the class to allow manipulating
    # them for testing and debugging
    storage_root = None

    def __init__(self, filename, path, force=False, force_md5=False,
                 after=None, no_defer=False, batch=None, header_update=None,
                 md5_before_header_update=None,
                 md5_after_header_update=None):
        """
        Create an :class:`~orm.ingestqueue.IngestQueue` instance with the
        given filename and path

        Parameters
        ----------
        filename : str
            Name of the file to ingest
        path : str
            Path of the file within the `storage_root`
        force: bool
            Whether to force reingestion
        force_md5: bool
            Whether for force checking of the file md5sum irrespective of the
            lastmod timestamp on the filesystem in deciding whether to reingest
        after: datetime.datetime
            Do not ingest this file until after this timestamp.
        no_defer: bool
            If set to true, do not defer ingesting this file even if it was
            recently modified.
        header_update: dict
            Header updates from a call to the update_headers API that resulted
            in this request for ingest. We pass this on to the export queue
            as a hint to try and avoid retransmitting the whole file
        md5_after_header_update: str
            md5sum of the file after the header update
        md5_before_header_update: str
            md5sum of the file before the header update
        """

        self.filename = filename
        self.path = path
        self.added = utcnow()
        self.inprogress = False
        self.force_md5 = force_md5
        self.force = force
        self.after = after if after is not None else self.added
        self.no_defer = no_defer
        self.batch = batch
        self.fail_dt = self.fail_dt_false  # See note in OrmQueueMixin
        self.sortkey = self.sortkey_from_filename()
        self.header_update = header_update
        self.md5_before_header_update = md5_before_header_update
        self.md5_after_header_update = md5_after_header_update

        fsc = get_config()
        self.storage_root = fsc.storage_root

    def __repr__(self):
        """
        Build a string representation of this :class:`~IngestQueueEntry` record

        Returns
        -------
            str : String representation of the :class:`~IngestQueueEntry` record
        """
        return f"<IngestQueue('{self.id}', '{self.filename}')>"

    def defer(self):
        """
        Examines the file to be ingested and decides whether to defer ingestion.
        There are two reasons we would defer it:
        1) The file was modified (according to the filesystem mtime) within
        the last "defer_threshold" seconds.
        2) The file is locked.

        If we do defer it, we defer it for defer_delay seconds.

        defer_threshold and defer_delay come from the configuration system.

        Returns
        -------
        - A string containing an explanatory message, if the file should be
        deferred
        - None otherwise

        Note that if the file should be deferred, we set the 'after' value of
        the object to reflect the appropriate delay, but we do not commit the
        object to the session, it is up to the caller to do that.
        """
        if self.no_defer:
            return None

        fsc = get_config()
        if fsc.defer_threshold == 0:
            return None

        # lastmod is in local timezone.
        age = datetime.datetime.now() - self.filelastmod
        threshold = datetime.timedelta(seconds=fsc.defer_threshold)
        delay = datetime.timedelta(seconds=fsc.defer_delay)
        message = None

        if age < threshold:
            # Defer ingestion of this file for defer_secs. 'after' is in UTC
            message = "Deferring ingestion of recently modified file " \
                      f"{self.filename} for {fsc.defer_delay} seconds"
            self.after = utcnow() + delay

        elif self.file_is_locked:
            message = f"Deferring ingestion of locked file {self.filename} " \
                      f"for {fsc.defer_delay} seconds"
            self.after = utcnow() + delay

        return message
