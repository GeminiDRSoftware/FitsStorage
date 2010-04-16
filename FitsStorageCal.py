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
  instheader = None

  def __init__(self, session, target, header=None, instheader=None):
    """
    Initialise a calibration manager for a given science data file
    Need to pass in an sqlalchemy session that should already be open
    This class will not close it
    Pass in the filename
    or optionally a header object and the appropiate instrument header (eg gmos) object
    """
    self.session = session
    self.target = target
    if(header == None):
      self.getheader()
    else:
      self.header = header
      self.target = self.header.diskfile.file.filename
    if(instheader == None):
      self.getinstheader()
    else:
      self.instheader = instheader

  def getheader(self):
    """
    Initialse the header data member of the class with a 
    FitsStorage Header table orm instance
    """
    query = self.session.query(Header).select_from(join(Header, join(DiskFile, File))).filter(File.filename==self.target).order_by(desc(DiskFile.lastmod)).limit(1)
    self.header = query.first()

    if('GMOS' in self.header.instrument):
      query = self.session.query(Gmos).filter(Gmos.header_id==self.header.id).limit(1)
      self.instheader = query.first()

  def getinstheader(self):
    """
    Initialse the inst header data member of the class with a 
    FitsStorage instrument header (eg Gmos) table orm instance
    """
    if('GMOS' in self.header.instrument):
      query = self.session.query(Gmos).filter(Gmos.header_id==self.header.id).limit(1)
      self.instheader = query.first()

  def arc(self, sameprog=False):
    if('GMOS' in self.header.instrument):
      return self.gmos_arc(sameprog)
    else:
      return None

  def gmos_arc(self, sameprog=False):
    query = self.session.query(Header).select_from(join(join(Gmos, Header), DiskFile))
    query = query.filter(Header.obstype=='ARC')

    # Search only the canonical (latest) entries
    query = query.filter(DiskFile.canonical==True)

    # Knock out the FAILs
    query = query.filter(Header.rawgemqa!='BAD')

    # Must Totally Match: Instrument, disperser
    query = query.filter(Header.instrument==self.header.instrument).filter(Gmos.disperser==self.instheader.disperser)
    # Must Match cwave 
    query = query.filter(Header.cwave==self.header.cwave)

    # Must match fpmask only if it's not the 5.0arcsec slit in the target, otherwise any longslit is OK
    if(self.header.fpmask != '5.0arcsec'):
      query = query.filter(Header.fpmask==self.header.fpmask)
    else:
      query = query.filter(Header.fpmask.like('%arcsec'))

    # Must match ccd binning
    query = query.filter(Gmos.xccdbin==self.instheader.xccdbin).filter(Gmos.yccdbin==self.instheader.yccdbin)

    # The science amproa must be equal or substring of the arc amproa
    query = query.filter(Gmos.amproa.like('%'+self.instheader.amproa+'%'))

    # Should we insist on the program ID matching?
    if(sameprog):
      query = query.filter(Header.progid==self.header.progid)

    # Order by absolute time separation. Maybe there's a better way to do this
    query = query.order_by("ABS(EXTRACT(EPOCH FROM (header.utdatetime - :utdatetime_x)))").params(utdatetime_x= self.header.utdatetime)

    # For now, we only want one result - the closest in time
    query = query.limit(1)

    return query.first()
