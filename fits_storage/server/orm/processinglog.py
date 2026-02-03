import datetime
import time

from sqlalchemy import Column
from sqlalchemy import BigInteger, Integer, Text, DateTime, Float, Boolean

from fits_storage.core.orm import Base
from fits_storage import utcnow

class ProcessingLog(Base):
    """
    This is a reduction log. Note unlike most of the other log tables this is
    not related to the web server, it logs some values from data processing
    jobs
    """
    __tablename__ = 'processinglog'

    id = Column(BigInteger, primary_key=True)
    filenames = Column(Text)
    recipe = Column(Text)
    processing_tag = Column(Text)
    num_raw_files = Column(Integer)
    num_reduced_files = Column(Integer)
    processing_started = Column(DateTime(timezone=False), index=True)
    processing_completed = Column(DateTime(timezone=False))
    cpu_secs = Column(Float)
    failed = Column(Boolean)
    log = Column(Text)

    def __init__(self, rqe):
        """
        Instantiate a processing log instance. Get values from rqe passed
        """
        self.processing_tag = rqe.tag
        self.filenames = ', '.join(rqe.filenames)
        self.recipe = rqe.recipe
        self.num_raw_files = len(rqe.filenames)

        self.processing_started = utcnow()
        self.start_process_time = time.process_time()

    def end(self, num_reduced_files, failed):
        self.processing_completed = utcnow()
        self.cpu_secs = time.process_time() - self.start_process_time
        self.num_reduced_files = num_reduced_files
        self.failed = failed