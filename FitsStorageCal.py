"""
This module provides calibration handling
"""

from FitsStorage import *
import FitsStorageConfig
import GeminiMetadataUtils

def get_cal_object(session, filename, header=None):
  """
  This function returns an appropriate calibration object for the given dataset
  Need to pass in an sqlalchemy session that should already be open, the class will not close it
  Also pass either a filename or a header object instance
  """

  # Did we get a header?
  if(header==None):
    # Get the header object from the filename
    query = session.query(Header).select_from(join(Header, join(DiskFile, File))).filter(File.filename==filename).order_by(desc(DiskFile.lastmod)).limit(1)
    header = query.first()

  # OK, now instantiate the appropriate Calibration object and return it
  c = None
  if('GMOS' in header.instrument):
    c = CalibrationGMOS(session, header)
  if('NIRI' == header.instrument):
    c = CalibrationNIRI(session, header)
  # if('NIFS' == header.instrument):
    # c = CalibrationNIFS(session, header)
  # Add other instruments here
  if(c==None):
    c = Calibration(session, header)

  return c

class Calibration():
  """
  This class provides a basic Calibration Manager
  This is the superclass from which the instrument specific variants subclass
  """

  session = None
  header = None
  required = []

  def __init__(self, session, header):
    """
    Initialise a calibration manager for a given header object (ie data file)
    Need to pass in an sqlalchemy session that should already be open, this class will not close it
    Also pass in a header object
    """
    self.session = session
    self.header = header

  def arc(self):
    return "arc method not defined for this instrument"

  def bias(self):
    return "bias method not defined for this instrument"

class CalibrationGMOS(Calibration):
  """
  This class implements a calibration manager for GMOS.
  It is a subclass of Calibration
  """
  gmos = None

  def __init__(self, session, header):
    # Init the superclass
    Calibration.__init__(self, session, header)

    # Find the gmosheader
    query = session.query(Gmos).filter(Gmos.header_id==self.header.id)
    self.gmos = query.first()

    # Set the list of required calibrations
    self.required = self.required()

  def required(self):
    # Return a list of the calibrations required for this GMOS dataset
    list=[]

    # BIASes do not require a bias. 
    if(self.header.obstype != 'BIAS'):
      list.append('bias')

    # If it (is spectroscopy) and (is not an OBJECT) and (is not a Twilight) then it needs an arc
    if((self.header.spectroscopy == True) and (self.header.obstype == 'OBJECT') and (self.header.object != 'Twilight')):
      list.append('arc')

    return list

  def arc(self, sameprog=False):
    query = self.session.query(Header).select_from(join(join(Gmos, Header), DiskFile))
    query = query.filter(Header.obstype=='ARC')

    # Search only the canonical (latest) entries
    query = query.filter(DiskFile.canonical==True)

    # Knock out the FAILs
    query = query.filter(Header.rawgemqa!='BAD')

    # Must Totally Match: Instrument, disperser
    query = query.filter(Header.instrument==self.header.instrument).filter(Gmos.disperser==self.gmos.disperser)

    # Must match filter (from KR 20100423)
    query = query.filter(Gmos.filtername==self.gmos.filtername)

    # Must Match cwave 
    query = query.filter(Header.cwave==self.header.cwave)

    # Must match fpmask only if it's not the 5.0arcsec slit in the target, otherwise any longslit is OK
    if(self.header.fpmask != '5.0arcsec'):
      query = query.filter(Header.fpmask==self.header.fpmask)
    else:
      query = query.filter(Header.fpmask.like('%arcsec'))

    # Must match ccd binning
    query = query.filter(Gmos.xccdbin==self.gmos.xccdbin).filter(Gmos.yccdbin==self.gmos.yccdbin)

    # The science amproa must be equal or substring of the arc amproa
    query = query.filter(Gmos.amproa.like('%'+self.gmos.amproa+'%'))

    # Should we insist on the program ID matching?
    if(sameprog):
      query = query.filter(Header.progid==self.header.progid)

    # Order by absolute time separation. Maybe there's a better way to do this
    query = query.order_by("ABS(EXTRACT(EPOCH FROM (header.utdatetime - :utdatetime_x)))").params(utdatetime_x= self.header.utdatetime)

    # For now, we only want one result - the closest in time
    query = query.limit(1)

    return query.first()

  def bias(self):
    query = self.session.query(Header).select_from(join(join(Gmos, Header), DiskFile))
    query = query.filter(Header.obstype=='BIAS')

     # Search only the canonical (latest) entries
    query = query.filter(DiskFile.canonical==True)

    # Knock out the FAILs
    query = query.filter(Header.rawgemqa!='BAD')

    # Must totally match instrument, xccdbin, yccdbin, readspeedmode, gainmode
    query = query.filter(Header.instrument==self.header.instrument)
    query = query.filter(Gmos.xccdbin==self.gmos.xccdbin).filter(Gmos.yccdbin==self.gmos.yccdbin)
    query = query.filter(Gmos.readspeedmode==self.gmos.readspeedmode).filter(Gmos.gainmode==self.gmos.gainmode)

    # The science amproa must be equal or substring of the arc amproa
    query = query.filter(Gmos.amproa.like('%'+self.gmos.amproa+'%'))

    # Order by absolute time separation. Maybe there's a better way to do this
    query = query.order_by("ABS(EXTRACT(EPOCH FROM (header.utdatetime - :utdatetime_x)))").params(utdatetime_x= self.header.utdatetime)

    # For now, we only want one result - the closest in time
    query = query.limit(1)

    return query.first()

class CalibrationNIRI(Calibration):
  """
  This class implements a calibration manager for NIRI.
  It is a subclass of Calibration
  """
  niri = None

  def __init__(self, session, header):
    # Init the superclass
    Calibration.__init__(self, session, header)

    # Find the niriheader
    query = session.query(Niri).filter(Niri.header_id==self.header.id)
    self.niri = query.first()

    # Set the list of required calibrations
    self.required = self.required()

  def required(self):
    # Return a list of the calibrations required for this NIRI dataset
    list=[]

    # Science Imaging OBJECTs require a DARK
    if((self.header.obstype == 'OBJECT') and (self.header.spectroscopy == False) and (self.header.obsclass=='science')):
      list.append('dark')

    return list

  def dark(self):
    query = self.session.query(Header).select_from(join(join(Niri, Header), DiskFile))
    query = query.filter(Header.obstype=='DARK')

    # Search only canonical entries
    query = query.filter(DiskFile.canonical == True)

    # Knock out the FAILs
    query = query.filter(Header.rawgemqa!='BAD')

    # Must totally match: detsec, readmode, welldepthmode, exptime, coadds
    query = query.filter(Niri.detsec == self.niri.detsec)
    query = query.filter(Niri.readmode == self.niri.readmode).filter(Niri.welldepthmode == self.niri.welldepthmode)
    query = query.filter(Header.exptime == self.header.exptime).filter(Niri.coadds == self.niri.coadds)

    # Order by absolute time separation. Maybe there's a better way to do this
    query = query.order_by("ABS(EXTRACT(EPOCH FROM (header.utdatetime - :utdatetime_x)))").params(utdatetime_x= self.header.utdatetime)

    # For now, we only want one result - the closest in time
    query = query.limit(1)

    return query.first()


