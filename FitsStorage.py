"""
This module contains the ORM classes for the tables in the fits storage
database.
"""
import sys

sys.path.append('/opt/gemini_python')

import sqlalchemy
import sqlalchemy.orm
import os
import datetime
import dateutil.parser
import zlib
import re

from sqlalchemy import Table, Column, MetaData, ForeignKey
from sqlalchemy import desc, func
from sqlalchemy import Integer, String, Boolean, Text, DateTime, Time, Date, Numeric
from sqlalchemy.databases.postgres import PGBigInteger

from sqlalchemy.orm import relation, backref, join

from sqlalchemy.ext.declarative import declarative_base

import FitsVerify
import CadcCRC
import CadcWMD

from FitsStorageConfig import *

from astrodata.AstroData import AstroData

# This was to debug the number of open database sessions.
#import logging
#logging.basicConfig(filename='/data/autoingest/debug.log', level=logging.DEBUG)
#logging.getLogger('sqlalchemy.pool').setLevel(logging.INFO)


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
  canonical = Column(Boolean, index=True)
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
    self.canonical = True
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
  instrument = Column(Text, index=True)
  utdatetime = Column(DateTime(timezone=False), index=True)
  localtime = Column(Time(timezone=False))
  obstype = Column(Text, index=True)
  obsclass = Column(Text, index=True)
  observer = Column(Text)
  ssa = Column(Text)
  object = Column(Text)
  ra = Column(Numeric(precision=16, scale=12))
  dec = Column(Numeric(precision=16, scale=12))
  azimuth = Column(Numeric(precision=16, scale=12))
  elevation = Column(Numeric(precision=16, scale=12))
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
  rawgemqa = Column(Text, index=True)
  qastate = Column(Text)
  release = Column(Date(TimeZone=False))
  reduction = Column(Text)
  fulltext = Column(Text)

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
      ad=AstroData(fullpath, mode='readonly')
      # Full header text first
      self.fulltext = ""
      self.fulltext += "Full Path Filename: " +  diskfile.file.fullpath() + "\n\n"
      self.fulltext += "AstroData Types: " +str(ad.types) + "\n\n"
      for i in range(len(ad.hdulist)):
        self.fulltext += "\n--- HDU %s ---\n" % i
        self.fulltext += str(ad.hdulist[i].header.ascardlist())
        self.fulltext += '\n'

      # Basic data identification part
      try:
        self.progid = ad.program_id()
      except KeyError:
        pass
      try:
        self.obsid = ad.observation_id()
      except KeyError:
        pass
      try:
        self.datalab = ad.data_label()
      except KeyError:
        pass
      try:
        self.telescope = ad.telescope()
      except KeyError:
        pass
      try:
        self.instrument = ad.instrument()
      except KeyError:
        pass

      # Date and times part
      try:
        datestring = ad.ut_date()
        timestring = ad.ut_time()
        if(datestring and timestring):
          datetime_string = "%s %s" % (datestring, timestring)
          self.utdatetime = dateutil.parser.parse(datetime_string)
        localtime_string = ad.local_time()
        if(localtime_string):
          # This is a bit of a hack so as to use the nice parser
          self.localtime = dateutil.parser.parse("2000-01-01 %s" % (localtime_string)).time()
      except KeyError:
        pass

      # Data Types
      try:
        self.obstype = ad.observation_type()
      except KeyError:
        pass
      try:
        self.obsclass = ad.observation_class()
      except KeyError:
        pass
      try:
        self.observer = ad.observer()
      except KeyError:
        pass
      try:
        self.ssa = ad.ssa()
      except KeyError:
        pass
      try:
        self.object = ad.object()
      except KeyError:
        pass
      try:
        self.ra = ad.ra()
      except KeyError:
        pass
      try:
        self.dec = ad.dec()
      except KeyError:
        pass
      try:
        self.azimuth = ad.azimuth()
      except KeyError:
        pass
      try:
        self.elevation = ad.elevation()
      except KeyError:
        pass
      try:
        self.crpa = ad.cass_rotator_pa()
      except KeyError:
        pass
      try:
        self.airmass = ad.airmass()
      except KeyError:
        pass
      try:
        self.rawiq = ad.raw_iq()
      except KeyError:
        pass
      try:
        self.rawcc = ad.raw_cc()
      except KeyError:
        pass
      try:
        self.rawwv = ad.raw_wv()
      except KeyError:
        pass
      try:
        self.rawbg = ad.raw_bg()
      except KeyError:
        pass
      try:
        self.rawpireq = ad.raw_pi_requirement()
      except KeyError:
        pass
      try:
        self.rawgemqa = ad.raw_gemini_qa()
      except KeyError:
        pass
      try:
        self.filter = ad.filter_name(pretty=True)
      except KeyError:
        pass
      try:
        self.exptime = ad.exposure_time()
      except KeyError:
        pass
      try:
        self.disperser = ad.disperser(pretty=True)
      except KeyError:
        pass
      try:
        self.cwave = ad.central_wavelength()
      except KeyError:
        pass
      try:
        self.fpmask = ad.focal_plane_mask()
      except KeyError:
        pass

      # Hack the AO header for now
      aofold = ad.phuHeader('AOFOLD')
      self.adaptive_optics = (aofold == 'IN')

      # And the Spectroscopy header
      self.spectroscopy = False
      if('SPECT' in ad.types):
        self.spectroscopy = True
  
      # Set the derived QA state and release date
      try:
        self.qastate = ad.qa_state()
      except KeyError:
        pass
      try:
        reldatestring = ad.release_date()
        if(reldatestring):
          reldts = "%s 00:00:00" % reldatestring
          self.release = dateutil.parser.parse(reldts).date()
      except KeyError:
        pass


      # Set the reduction state
      self.reduction = 'RAW'
      if('PREPARED' in ad.types):
        self.reduction = 'PREPARED'
      if('PROCESSED_FLAT' in ad.types):
        self.reduction = 'PROCESSED_FLAT'
      if('PROCESSED_BIAS' in ad.types):
        self.reduction = 'PROCESSED_BIAS'
  
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
  firstwrite = Column(DateTime(timezone=True))
  lastwrite = Column(DateTime(timezone=True))
  lastverified = Column(DateTime(timezone=True))
  location = Column(Text)
  lastmoved = Column(DateTime(timezone=True))
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
  tape = relation(Tape, order_by=id)
  filenum = Column(Integer)
  startdate = Column(DateTime(timezone=True))
  enddate = Column(DateTime(timezone=True))
  suceeded = Column(Boolean)
  size = Column(Integer)
  beforestatus = Column(Text)
  afterstatus = Column(Text)
  hostname = Column(Text)
  tapedrive = Column(Text)
  notes = Column(Text)

  
class TapeFile(Base):
  """
  This is the ORM object for the TapeFile table
  """
  __tablename__ = 'tapefile'

  id = Column(Integer, primary_key=True)
  tapewrite_id = Column(Integer, ForeignKey('tapewrite.id'), nullable=False)
  tapewrite = relation(TapeWrite, order_by=id)
  filename = Column(Text)
  size = Column(Integer)
  ccrc = Column(Text)
  md5sum = Column(Text)
  lastmod = Column(DateTime(timezone=True))

class Gmos(Base):
  """
  This is the ORM object for the GMOS details.
  This is used for both GMOS-N and GMOS-S
  """
  __tablename__ = 'gmos'

  id = Column(Integer, primary_key=True)
  header_id = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
  header = relation(Header, order_by=id)
  disperser = Column(Text, index=True)
  filtername = Column(Text, index=True)
  xccdbin = Column(Integer, index=True)
  yccdbin = Column(Integer, index=True)
  amproa = Column(Text, index=True)
  readspeedmode = Column(Text, index=True)
  gainmode = Column(Text, index=True)

  def __init__(self, header):
    self.header = header

    # Populate from the astrodata object
    self.populate()

  def populate(self):
    # Get an AstroData object on it
    try:
      ad = AstroData(self.header.diskfile.file.fullpath(), mode="readonly")
      # Populate values
      try:
        self.disperser = ad.disperser()
      except KeyError:
        pass
      try:
        self.filtername = ad.filter_name()
      except KeyError:
        pass
      try:
        self.xccdbin = ad.detector_x_bin()
      except (KeyError, IndexError):
        pass
      try:
        self.yccdbin = ad.detector_y_bin()
      except (KeyError, IndexError):
        pass
      try:
        self.amproa = str(ad.amp_read_area(asList=True))
      except (KeyError, IndexError):
        pass
      try:
        self.readspeedmode = ad.read_speed_mode()
      except (KeyError, IndexError):
        pass
      try:
        self.gainmode = ad.gain_mode()
      except (KeyError, IndexError):
        pass
      ad.close()
    except:
      # Astrodata open failed
      pass

class Niri(Base):
  """
  This is the ORM object for the NIRI details
  """
  __tablename__ = 'niri'

  id = Column(Integer, primary_key=True)
  header_id = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
  header = relation(Header, order_by=id)
  disperser = Column(Text, index=True)
  filtername = Column(Text, index=True)
  readmode = Column(Text, index=True)
  welldepthmode = Column(Text, index=True)
  detsec = Column(Text, index=True)
  coadds = Column(Integer, index=True)

  def __init__(self, header):
    self.header = header

    # Populate from an astrodata object
    self.populate()

  def populate(self):
    # Get an AstroData object on it
    try:
      ad = AstroData(self.header.diskfile.file.fullpath(), mode="readonly")
      # Populate values
      try:
        self.disperser = ad.disperser()
      except KeyError:
        pass
      try:
        self.filtername = ad.filter_name()
      except KeyError:
        pass
      try:
        self.readmode = ad.read_mode()
      except KeyError:
        pass
      try:
        self.welldepthmode = ad.well_depth_mode()
      except KeyError:
        pass
      try:
        self.detsec = ad.detector_section()
      except KeyError:
        pass
      try:
        self.coadds = ad.coadds()
      except KeyError:
        pass
      ad.close()
    except:
      # Astrodata open failed
      pass

class PhotStandard(Base):
  """
  This is the ORM class for the table holding the standard star list for the instrument monitoring
  """
  __tablename__ = 'standards'

  id = Column(Integer, primary_key=True)
  name = Column(Text)
  field = Column(Text)
  ra = Column(Numeric(precision=16, scale=12), index=True)
  dec = Column(Numeric(precision=16, scale=12), index=True)
  u_mag = Column(Numeric(precision=6, scale=4))
  v_mag = Column(Numeric(precision=6, scale=4))
  g_mag = Column(Numeric(precision=6, scale=4))
  r_mag = Column(Numeric(precision=6, scale=4))
  i_mag = Column(Numeric(precision=6, scale=4))
  z_mag = Column(Numeric(precision=6, scale=4))
  y_mag = Column(Numeric(precision=6, scale=4))
  j_mag = Column(Numeric(precision=6, scale=4))
  h_mag = Column(Numeric(precision=6, scale=4))
  k_mag = Column(Numeric(precision=6, scale=4))
  lprime_mag = Column(Numeric(precision=6, scale=4))
  m_mag = Column(Numeric(precision=6, scale=4))
