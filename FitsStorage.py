import sys

import sqlalchemy
import sqlalchemy.orm
import os
import pyfits
import datetime
import dateutil.parser
import zlib
import re

from sqlalchemy import Table, Column, MetaData, ForeignKey
from sqlalchemy import desc
from sqlalchemy import Integer, String, Boolean, Text, DateTime, Time, Numeric
from sqlalchemy.databases.postgres import PGBigInteger

from sqlalchemy.orm import relation, backref, join

from sqlalchemy.ext.declarative import declarative_base

import FitsVerify
import CadcCRC

Base = declarative_base()

# Configure the path to the storage root here for now
storage_root = '/data/dataflow'

# We need to handle the database connection in here too so that the
# orm can properly handle the relations defined in the database
# at this level rather than in the main script

# Create a databas engine connection to the postgres database
# and an sqlalchemy session to go with it
pg_db = sqlalchemy.create_engine('postgres:///pytest')
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

  def __init__(self, file):
    self.file_id = file.id
    self.present = True
    self.entrytime = 'now'
    self.size = file.size()
    self.ccrc = file.ccrc()
    self.lastmod = file.lastmod()
    self.fits_verify(file)

  def __repr__(self):
    return "<DiskFile('%s', '%s')>" %(self.id, self.file_id)

  def fits_verify(self, file):
    list = FitsVerify.fitsverify(file.fullpath())
    self.isfits = bool(list[0])
    self.fvwarnings = list[1]
    self.fverrors = list[2]
    self.fvreport = list[3]
    

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
  rawiq = Column(Text)
  rawcc = Column(Text)
  rawwv = Column(Text)
  rawbg = Column(Text)
  rawpireq = Column(Text)
  rawgemqa = Column(Text)
  

  def __init__(self, diskfile):
    self.diskfile_id = diskfile.id
    self.populate_fits(diskfile)

  def __repr__(self):
    return "<Header('%s', '%s')>" %(self.id, self.diskfile_id)

  def populate_fits(self, diskfile):
    fullpath = diskfile.file.fullpath()
    # Try and open it as a fits file
    hdulist=[]
    try:
      hdulist = pyfits.open(fullpath)
    except:
      print "Not a valid FITS file - not attempting to read headers"
    if(len(hdulist)):
      self.progid = self.get_header(hdulist[0], 'GEMPRGID')
      self.obsid = self.get_header(hdulist[0], 'OBSID')
      self.datalab = self.get_header(hdulist[0], 'DATALAB')
      self.telescope = self.get_header(hdulist[0], 'TELESCOP')
      self.instrument = self.get_header(hdulist[0], 'INSTRUME')
      datestring = self.get_header(hdulist[0], 'DATE-OBS')
      timestring = self.get_header(hdulist[0], 'TIME-OBS')
      if(not timestring):
        timestring = self.get_header(hdulist[0], 'UT')
      if(datestring and timestring):
        datetime_string = "%s %s" % (datestring, timestring)
        self.utdatetime = dateutil.parser.parse(datetime_string)
      if(not datestring):
        # Bleah. Bodge it from the filename for now
        fn = diskfile.file.filename
        datestring = fn[1:9]
        if(re.match('20\d\d[01]\d[0123]\d', datestring)):
          # Assume it's a valid datestring
          # We'll stick these at 23:59:59.99 to put them at the end of the day
          datetime_string = "%s %s" % (datestring, '23:59:59.99')
          self.utdatetime = dateutil.parser.parse(datetime_string)
      localtime_string = self.get_header(hdulist[0], 'LT')
      if(localtime_string):
        # This is a bit of a hack so as to use the nice parser
        self.localtime = dateutil.parser.parse("2000-01-01 %s" % (localtime_string)).time()
      self.obstype = self.get_header(hdulist[0], 'OBSTYPE')
      self.obsclass = self.get_header(hdulist[0], 'OBSCLASS')
      self.observer = self.get_header(hdulist[0], 'OBSERVER')
      self.ssa = self.get_header(hdulist[0], 'SSA')
      self.object = self.get_header(hdulist[0], 'OBJECT')
      self.ra = self.get_header(hdulist[0], 'RA')
      self.dec = self.get_header(hdulist[0], 'DEC')
      self.az = self.get_header(hdulist[0], 'AZIMUTH')
      self.el = self.get_header(hdulist[0], 'ELEVATIO')
      self.crpa = self.get_header(hdulist[0], 'CRPA')
      self.airmass = self.get_header(hdulist[0], 'AIRMASS')
      self.rawiq = self.get_header(hdulist[0], 'RAWIQ')
      self.rawcc = self.get_header(hdulist[0], 'RAWCC')
      self.rawwv = self.get_header(hdulist[0], 'RAWWV')
      self.rawbg = self.get_header(hdulist[0], 'RAWBG')
      self.rawpireq = self.get_header(hdulist[0], 'RAWPIREQ')
      self.rawgemqa = self.get_header(hdulist[0], 'RAWGEMQA')
    else:
      print "Not a valid FITS file - not attempting to read headers"
    hdulist.close()
     

  def get_header(self, hdu, keyword):
    # If the keyword is not present, do not return anything
    # This is better than returning an emtpy string as it works with numeric types too
    try:
      val = hdu.header[keyword]
      if(val):
        return(val)
    except:
      print "keyword not present: ", keyword
