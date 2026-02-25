import os.path
import time

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime, Float, Boolean
from sqlalchemy.orm import relationship

from fits_storage.core.orm import Base
from fits_storage import utcnow

from fits_storage.config import get_config
fsc = get_config()

class ProcessingLog(Base):
    """
    This is a reduction log. Note unlike most of the other log tables this is
    not related to the web server, it logs some values from data processing
    jobs
    """
    __tablename__ = 'processinglog'

    id = Column(Integer, primary_key=True)
    input_files = Column(Text)
    output_files = Column(Text)
    num_input_files = Column(Integer)
    num_output_files = Column(Integer)
    capture_files = Column(Boolean)
    capture_monitoring = Column(Boolean)
    debundle = Column(Text)
    recipe = Column(Text)
    processing_tag = Column(Text, index=True)
    processing_started = Column(DateTime(timezone=False))
    processing_completed = Column(DateTime(timezone=False))
    cpu_secs = Column(Float)
    failed = Column(Boolean, index=True)
    log = Column(Text)

    def __init__(self, rqe):
        """
        Instantiate a processing log instance. Get values from rqe passed
        """
        self.processing_tag = rqe.tag
        self.input_files = ', '.join(rqe.filenames)
        self.num_input_files = len(rqe.filenames)
        self.recipe = rqe.recipe
        self.debundle = rqe.debundle
        self.capture_files = rqe.capture_files
        self.capture_monitoring = rqe.capture_monitoring

        self.processing_started = utcnow()
        self.start_process_time = time.process_time()

    def end(self, reduced_files, failed):
        self.processing_completed = utcnow()
        self.cpu_secs = time.process_time() - self.start_process_time
        output_files = [os.path.basename(i) for i in reduced_files]
        self.output_files = ', '.join(output_files)
        self.num_output_files = len(reduced_files)
        self.failed = failed

class ProcessingLogFile(Base):
    """
    This table identifies files for the processing log. There should be 1
    entry here for each input and output file relevant to a ProcessingLog
    entry. This table facilitates efficient searching for ProcessingLogs by
    filename of input and output files.

    For simplicity, we add these to the session directly in reducer.py rather
    than using the SQLalchemy relationship to add them, as we have to
    calculate the md5 and set input vs output...

    We do use the relationship to find the processinglog associated with a
    processinglogfile when we generate the summary table links.
    """
    __tablename__ = 'processinglog_files'

    id = Column(Integer, primary_key=True)
    processinglog_id = Column(Integer, ForeignKey('processinglog.id'),
                              nullable=False, index=True)
    processinglog = relationship(ProcessingLog, order_by=id)

    filename = Column(Text, index=True)
    md5sum = Column(Text, index=True)  # We uncompress before processing so this is data_md5
    output = Column(Boolean, index=True)  # True for output file, False for input file

    def __init__(self, plid, filename=None, md5sum=None, output=None):
        self.processinglog_id = plid
        self.filename = filename
        self.md5sum = md5sum
        self.output = output
