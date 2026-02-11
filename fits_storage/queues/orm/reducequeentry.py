"""
This module contains the ReduceQueueEntry ORM class.

"""
import datetime

from sqlalchemy import Column, UniqueConstraint, Enum
from sqlalchemy import Integer, Boolean, DateTime, Text, ARRAY, Float

from fits_storage.core.orm import Base
from fits_storage import utcnow
from .ormqueuemixin import OrmQueueMixin


from fits_storage.config import get_config

debundle_options = ['INDIVIDUAL', 'ALL', 'GHOST', 'GHOST-SLIT', 'GHOST-REDBLUE',
                    'IGRINS-2']
DEBUNDLE_ENUM = Enum(*debundle_options, name='debundle_options')

class ReduceQueueEntry(OrmQueueMixin, Base):
    """
    This is the ORM object for the ReduceQueue table.

    The reducequeue holds a lists of files that are to be passed to a DRAGONS
    Reduce class for data reduction.
    """
    __tablename__ = 'reducequeue'
    __table_args__ = (
        UniqueConstraint('filenames', 'debundle', 'recipe', 'inprogress', 'fail_dt'),
    )

    id = Column(Integer, primary_key=True)
    # Note, ARRAY isn't supported by SQLite. Kludge this here so that the
    # table can be built for SQLite testing environments even though this table
    # would never be used in the real world on an SQLite install.
    fsc = get_config()
    if fsc.using_sqlite:
        filenames = Column(Text)
    else:
        filenames = Column(ARRAY(Text))
    inprogress = Column(Boolean, index=True)
    fail_dt = Column(DateTime, index=True)
    added = Column(DateTime)
    sortkey = Column(Text, index=True)
    filename = Column(Text)
    debundle = Column(DEBUNDLE_ENUM)
    recipe = Column(Text)
    uparms = Column(Text)
    capture_files = Column(Boolean)
    capture_monitoring = Column(Boolean)
    batch = Column(Text)
    after_batch = Column(Text)
    error = Column(Text)

    # These are used to ensure each machine servicing the queue does not take
    # on simultaneous jobs for which it doesn't have enough memory
    mem_gb = Column(Float) # Estimated Memory Footprint of reduce job
    host = Column(Text) # IP address or other ID of machine

    # Processing metadata passed into the reduced data
    intent = Column(Text)  # Goes into PROCITNT header
    initiatedby = Column(Text)  # Goes into PROCINBY header
    tag = Column(Text)  # Goes into PROCTAG header

    def __init__(self, filenames):
        """
        Create a :class:`~ReduceQueueEntry` record for the given list of
        filenames.

        Note that the 'filename' member of the ORM class is used for generic
        error reporting and logging. As such, we just set it to the first
        entry of the filenames list for convenience.


        Parameters
        ----------
        filenames : list of str
            The filenames for this reducequeue entry

        """
        self.filenames = filenames
        if self.fsc.using_sqlite:
            # Used only in tests.
            self.filename = self.filenames
        else:
            self.filename = filenames[0]
        self.inprogress = False
        self.sortkey = self.sortkey_from_filename()
        self.added = utcnow()
        self.fail_dt = self.fail_dt_false  # See Note in OrmQueueMixin
