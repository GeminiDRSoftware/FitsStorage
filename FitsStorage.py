"""
This module contains the ORM classes for the tables in the fits storage
database.
"""
import sys

sys.path.append('/data/extern/gemini_python')

import sqlalchemy
import sqlalchemy.orm
import os
import datetime
import dateutil.parser
import zlib
import re

from sqlalchemy import Table, Column, MetaData, ForeignKey
from sqlalchemy import desc, func
from sqlalchemy import Integer, String, Boolean, Text, DateTime, Time, Numeric
from sqlalchemy.databases.postgres import PGBigInteger

from sqlalchemy.orm import relation, backref, join

from sqlalchemy.ext.declarative import declarative_base

import FitsVerify
import CadcCRC
import CadcWMD

from FitsStorageConfig import *

from astrodata import AstroData

Base = declarative_base()

# We need to handle the database connection in here too so that the
# orm can properly handle the relations defined in the database
# at this level rather than in the main script

# Create a databas engine connection to the postgres database
# and an sqlalchemy session to go with it
pg_db = sqlalchemy.create_engine(fits_database)
sessionfactory = sqlalchemy.orm.sessionmaker(pg_db)

# Do not create the session here, these are not supposed to be global
#session = sessionfactory()


class File(Base):
  """
  This is the ORM class for the file table
  """
  __tablename__ = 'file'

  id = Column(Integer, primary_key=True)
  filename = Column(Text, nullable=False, unique=True, index=True)
  path = Column(Text)

  def __init__(self, filename, path):
    self.filename = filename
    self.path = path

  def __repr__(self):
    return "<File('%s', '%s')>" %(self.id, self.filename)

  def fullpath(self):
    return os.path.join(storage_root, self.path, self.filename)

  def exists(self):
    exists = os.access(self.fullpath(), os.F_OK | os.R_OK)
    isfile = os.path.isfile(self.fullpath())
    return (exists and isfile)

  def size(self):
    return os.path.getsize(self.fullpath())

  def ccrc(self):
    return CadcCRC.cadcCRC(self.fullpath())

  def lastmod(self):
    return datetime.datetime.fromtimestamp(os.path.getmtime(self.fullpath()))
    
class DiskFile(Base):
  """
  This is the ORM class for the diskfile table.
  """
  __tablename__ = 'diskfile'

  id = Column(Integer, primary_key=True)
  file_id = Column(Integer, ForeignKey('file.id'), nullable=False, index=True)
  file = relation(File, order_by=id)
  present = Column(Boolean, index=True)
  ccrc = Column(Text)
  size = Column(Integer)
  lastmod = Column(DateTime(timezone=True))
  entrytime = Column(DateTime(timezone=True))
  isfits = Column(Boolean)
  fvwarnings = Column(Integer)
  fverrors = Column(Integer)
  fvreport = Column(Text)
  wmdready = Column(Boolean)
  wmdreport = Column(Text)

  def __init__(self, file, skip_fv, skip_wmd):
    self.file_id = file.id
    self.present = True
    self.entrytime = 'now'
    self.size = file.size()
    self.ccrc = file.ccrc()
    self.lastmod = file.lastmod()
    if(skip_fv):
      self.fverrors=0
    else:
      self.fits_verify(file)
    if(skip_wmd):
      self.wmdready = True
    else:
      self.wmd(file)

  def __repr__(self):
    return "<DiskFile('%s', '%s')>" %(self.id, self.file_id)

  def fits_verify(self, file):
    """
    Calls the FitsVerify module and records the results
    in the current diskfile object / table row
    """
    list = FitsVerify.fitsverify(file.fullpath())
    self.isfits = bool(list[0])
    self.fvwarnings = list[1]
    self.fverrors = list[2]
    self.fvreport = list[3]
    
  def wmd(self, file):
    """
    Calls the CadcWMD module and records the results
    in the current diskfile object / table row
    """
    list = CadcWMD.cadcWMD(file.fullpath())
    self.wmdready = bool(list[0])
    self.wmdreport = list[1]

class Header(Base):
  """
  This is the ORM class for the Header table
  """
  __tablename__ = 'header'

  id = Column(Integer, primary_key=True)
  diskfile_id = Column(Integer, ForeignKey('diskfile.id'), nullable=False, index=True)
  diskfile = relation(DiskFile, order_by=id)
  progid = Column(Text, index=True)
  obsid = Column(Text, index=True)
  datalab = Column(Text, index=True)
  telescope = Column(Text)
  instrument = Column(Text)
  utdatetime = Column(DateTime(timezone=False), index=True)
  localtime = Column(Time(timezone=False))
  obstype = Column(Text)
  obsclass = Column(Text)
  observer = Column(Text)
  ssa = Column(Text)
  object = Column(Text)
  ra = Column(Numeric(precision=16, scale=12))
  dec = Column(Numeric(precision=16, scale=12))
  az = Column(Numeric(precision=16, scale=12))
  el = Column(Numeric(precision=16, scale=12))
  crpa = Column(Numeric(precision=16, scale=12))
  airmass = Column(Numeric(precision=8, scale=6))
  filter = Column(Text)
  exptime = Column(Numeric(precision=8, scale=4))
  disperser = Column(Text)
  cwave = Column(Numeric(precision=8, scale=6))
  fpmask = Column(Text)
  spectroscopy = Column(Boolean)
  adaptive_optics = Column(Boolean)
  rawiq = Column(Text)
  rawcc = Column(Text)
  rawwv = Column(Text)
  rawbg = Column(Text)
  rawpireq = Column(Text)
  rawgemqa = Column(Text)
  qastate = Column(Text)

  def __init__(self, diskfile):
    self.diskfile_id = diskfile.id
    self.populate_fits(diskfile)

  def __repr__(self):
    return "<Header('%s', '%s')>" %(self.id, self.diskfile_id)

  def populate_fits(self, diskfile):
    """
    Populates header table values from the FITS headers of the file.
    Uses the AstroData object to access the file.
    """
    fullpath = diskfile.file.fullpath()
    # Try and open it as a fits file
    ad=0
    try:
      ad=AstroData.AstroData(fullpath, mode='readonly')
      # Basic data identification part
      self.progid = ad.phuHeader('GEMPRGID')
      self.obsid = ad.phuHeader('OBSID')
      self.datalab = ad.phuHeader('DATALAB')
      self.telescope = ad.phuHeader('TELESCOP')
      self.instrument = ad.instrument()

      # Date and times part
      datestring = ad.utdate()
      timestring = ad.uttime()
      if(datestring and timestring):
        datetime_string = "%s %s" % (datestring, timestring)
        self.utdatetime = dateutil.parser.parse(datetime_string)
      localtime_string = ad.phuHeader('LT')
      if(localtime_string):
        # This is a bit of a hack so as to use the nice parser
        self.localtime = dateutil.parser.parse("2000-01-01 %s" % (localtime_string)).time()

      # Data Types
      self.obstype = ad.phuHeader('OBSTYPE')
      self.obsclass = ad.phuHeader('OBSCLASS')
      self.observer = ad.phuHeader('OBSERVER')
      self.ssa = ad.phuHeader('SSA')
      self.object = ad.phuHeader('OBJECT')
      self.ra = ad.phuHeader('RA')
      self.dec = ad.phuHeader('DEC')
      self.az = ad.phuHeader('AZIMUTH')
      self.el = ad.phuHeader('ELEVATIO')
      self.crpa = ad.phuHeader('CRPA')
      airmass = ad.airmass()
      if(airmass < 0):
        airmass = None
      self.airmass = airmass
      self.rawiq = ad.phuHeader('RAWIQ')
      self.rawcc = ad.phuHeader('RAWCC')
      self.rawwv = ad.phuHeader('RAWWV')
      self.rawbg = ad.phuHeader('RAWBG')
      self.rawpireq = ad.phuHeader('RAWPIREQ')
      self.rawgemqa = ad.phuHeader('RAWGEMQA')
      self.filter = ad.filtername(pretty=True)
      self.exptime = ad.exptime()
      self.disperser = ad.disperser()
      self.cwave = ad.cwave()
      self.fpmask = ad.fpmask()

      # Hack the AO header for now
      aofold = ad.phuHeader('AOFOLD')
      self.adaptive_optics = (aofold == 'IN')

      # And the Spectroscopy header
      self.spectroscopy = False
      if('NIFS' in ad.types):
        self.spectroscopy = True
        self.disperser = ad.disperser()[0:1]
      if('NIRI_SPECT' in ad.types):
        self.spectroscopy = True
        self.disperser = ad.disperser()[0:6]
      if('GMOS_SPECT' in ad.types):
        self.spectroscopy = True
        self.disperser = ad.disperser()[0:4]
  
      # and michelle for what it's worth
      if('MICHELLE' in ad.types):
        if(ad.phuHeader('CAMERA') == 'spectroscopy'):
          self.spectroscopy = True
  
      # Set the derived QA state
      self.qastate = "%s:%s" % (self.rawpireq, self.rawgemqa)
      if((self.rawpireq == 'UNKNOWN') and (self.rawgemqa == 'UNKNOWN')):
        self.qastate = 'Undefined'
      if((self.rawpireq == 'YES') and (self.rawgemqa == 'USABLE')):
        self.qastate = 'Pass'
      if((self.rawpireq == 'NO') and (self.rawgemqa == 'USABLE')):
        self.qastate = 'Usable'
      if((self.rawpireq == 'NO') and (self.rawgemqa == 'BAD')):
        self.qastate = 'Fail'
      if((self.rawpireq == 'CHECK') and (self.rawgemqa == 'CHECK')):
        self.qastate = 'CHECK'
  
      ad.close()
    except:
      # Astrodata open or any of the above failed
      pass

class IngestQueue(Base):
  """
  This is the ORM object for the IngestQueue table
  """
  __tablename__ = 'ingestqueue'

  id = Column(Integer, primary_key=True)
  filename = Column(Text, nullable=False, unique=False, index=True)
  path = Column(Text)
  inprogress = Column(Boolean, index=True)
  added = Column(DateTime)

  def __init__(self, filename, path):
    self.filename = filename
    self.path = path
    self.added = datetime.datetime.now()
    self.inprogress = False

  def __repr__(self):
    return "<IngestQueue('%s', '%s')>" %(self.id, self.filename)

class Tape(Base):
  """
  This is the ORM object for the Tape table
  Each row in this table represents a data tape
  """
  __tablename__ = 'tape'

  id = Column(Integer, primary_key=True)
  label = Column(Text, nullable=False, index=True)
  firstwrite = Column(DateTime)
  lastwrite = Column(DateTime)
  lastverified = Column(DateTime)
  location = Column(Text)
  lastmoved = Column(DateTime)
  active = Column(Boolean)
  fate = Column(Text)

  def __init__(self, label):
    self.label = label
    self.active = True

class TapeWrite(Base):
  """
  This is the ORM object for the TapeWrite table
  Each row in this table represents a tape writing session
  """

  __tablename__ = 'tapewrite'

  id = Column(Integer, primary_key=True)
  tape_id = Column(Integer, ForeignKey('tape.id'), nullable=False, index=True)
  startdate = Column(DateTime)
  enddate = Column(DateTime)
  command = Column(Text)
  commandStatus = Column(Integer)
  beforeStatus = Column(Text)
  afterStatus = Column(Text)
  hostname = Column(Text)
  tapedrive = Column(Text)
  notes = Column(Text)

  
class TapeFile(Base):
  """
  This is the ORM object for the TapeFile table
  """
  __tablename__ = 'tapefile'

  id = Column(Integer, primary_key=True)
  diskfile_id = Column(Integer, ForeignKey('diskfile.id'), nullable=False, index=True)
  tapewrite_id = Column(Integer, ForeignKey('tapewrite.id'), nullable=False)
