"""
This module provides calibration handling
"""

from FitsStorage import *
import FitsStorageConfig
import GeminiMetadataUtils


class Calibration:
  """
  This class provides a basic Calibration Manager
  """

  session = None
  target = None
  header = None

  def __init__(self, session, target, header=None):
    """
    Initialise a calibration manager for a given science data file
    Need to pass in an sqlalchemy session that should already be open
    This class will not close it
    Pass in the filename
    or optionally a header object
    """
    self.session = session
    self.target = target
    if(header == None):
      self.getheader()
    else:
      self.header = header
      self.target = self.header.diskfile.file.filename

  def getheader(self):
    """
    Initialse the header data member of the class with a 
    FitsStorage Header table orm instance
    """
    query = self.session.query(Header).select_from(join(Header, join(DiskFile, File))).filter(File.filename==self.target).order_by(desc(DiskFile.lastmod)).limit(1)
    self.header = query.first()

  def arc(self):
    if(self.header.instrument == 'GMOS-N' or self.header.instrument == 'GMOS-S'):
      return self.gmos_arc()
    else:
      return None

  def gmos_arc(self):
    query = self.session.query(Header).select_from(join(Header, DiskFile))
    query = query.filter(Header.obstype=='ARC')
    # For simplicity for now we require the file to be present.
    query = query.filter(DiskFile.present==True)

    # Must Totally Match: Instrument, disperser
    query = query.filter(Header.instrument==self.header.instrument).filter(Header.disperser==self.header.disperser)
    # Must Match cwave and fpmask
    query = query.filter(Header.fpmask==self.header.fpmask).filter(Header.cwave==self.header.cwave)

    # Order by absolute time separation. Maybe there's a better way to do this
    query = query.order_by("ABS(EXTRACT(EPOCH FROM (header.utdatetime - :utdatetime_x)))").params(utdatetime_x= self.header.utdatetime)

    # For now, we only want one result - the closest in time
    query = query.limit(1)

    return query.first()
