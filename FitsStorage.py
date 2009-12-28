import sys

sys.path.append('/data/extern/gemini_python')

import sqlalchemy
import sqlalchemy.orm
import os
import pyfits
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
session = sessionfactory()


class File(Base):
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
    list = FitsVerify.fitsverify(file.fullpath())
    self.isfits = bool(list[0])
    self.fvwarnings = list[1]
    self.fverrors = list[2]
    self.fvreport = list[3]
    
  def wmd(self, file):
    list = CadcWMD.cadcWMD(file.fullpath())
    self.wmdready = bool(list[0])
    self.wmdreport = list[1]

class Header(Base):
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
  ra = Column(Numeric)
  dec = Column(Numeric)
  az = Column(Numeric)
  el = Column(Numeric)
  crpa = Column(Numeric)
  airmass = Column(Numeric)
  filter = Column(Text)
  exptime = Column(Numeric)
  disperser = Column(Text)
  cwave = Column(Numeric)
  fpmask = Column(Text)
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
    fullpath = diskfile.file.fullpath()
    # Try and open it as a fits file
    ad=0
    try:
      ad=AstroData.AstroData(fullpath)
    except:
      print "Not a valid FITS file - not attempting to read headers"
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
    self.airmass = ad.airmass()
    self.rawiq = ad.phuHeader('RAWIQ')
    self.rawcc = ad.phuHeader('RAWCC')
    self.rawwv = ad.phuHeader('RAWWV')
    self.rawbg = ad.phuHeader('RAWBG')
    self.rawpireq = ad.phuHeader('RAWPIREQ')
    self.rawgemqa = ad.phuHeader('RAWGEMQA')
    self.filter = ad.filtername()
    self.exptime = ad.exptime()
    self.disperser = ad.disperser()
    self.cwave = ad.cwave()
    self.fpmask = ad.fpmask()
    ad.close()

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
