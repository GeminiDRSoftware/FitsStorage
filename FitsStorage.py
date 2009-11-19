import sqlalchemy
import sqlalchemy.orm
import os
import pyfits
import datetime
import dateutil.parser
import zlib

from sqlalchemy import Table, Column, MetaData, ForeignKey
from sqlalchemy import Integer, String, Boolean, Text, DateTime
from sqlalchemy.databases.postgres import PGBigInteger

from sqlalchemy.orm import relation, backref

from sqlalchemy.ext.declarative import declarative_base
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
  filename = Column(Text, nullable=False)
  path = Column(Text)

  def __init__(self, filename, path):
    self.filename = filename
    self.path = path

  def __repr__(self):
    return "<File('%s', '%s')>" %(self.id, self.filename)

  def fullpath(self):
    return os.path.join(storage_root, self.path, self.filename)

  def exists(self):
    return os.access(self.fullpath(), os.F_OK | os.R_OK)

  def size(self):
    return os.path.getsize(self.fullpath())

  def ccrc(self):
    f = open(self.fullpath(), "r")
    ccrc = zlib.crc32(f.read())
    f.close()
    return ccrc

  def lastmod(self):
    return datetime.datetime.fromtimestamp(os.path.getmtime(self.fullpath()))
    
class DiskFile(Base):
  __tablename__ = 'diskfile'

  id = Column(Integer, primary_key=True)
  file_id = Column(Integer, ForeignKey('file.id'), nullable=False)
  file = relation(File, order_by=id)
  present = Column(Boolean)
  ccrc = Column(PGBigInteger)
  size = Column(Integer)
  lastmod = Column(DateTime(timezone=True))
  entrytime = Column(DateTime(timezone=True))
  source = Column(Text)

  def __init__(self, file):
    self.file_id = file.id
    self.present = True
    self.entrytime = 'now'
    self.size = file.size()
    self.ccrc = file.ccrc()
    self.lastmod = file.lastmod()

  def __repr__(self):
    return "<DiskFile('%s', '%s')>" %(self.id, self.file_id)

class Header(Base):
  __tablename__ = 'header'

  id = Column(Integer, primary_key=True)
  diskfile_id = Column(Integer, ForeignKey('diskfile.id'), nullable=False)
  diskfile = relation(DiskFile, order_by=id)
  progid = Column(Text)
  obsid = Column(Text)
  datalab = Column(Text)
  telescope = Column(Text)
  instrument = Column(Text)
  utdatetime = Column(DateTime(timezone=False))
  obstype = Column(Text)

  def __init__(self, diskfile):
    self.diskfile_id = diskfile.id
    self.populate_fits(diskfile)

  def __repr__(self):
    return "<Header('%s', '%s')>" %(self.id, self.diskfile_id)

  def populate_fits(self, diskfile):
    fullpath = diskfile.file.fullpath()
    hdulist = pyfits.open(fullpath)
    self.progid = hdulist[0].header['GEMPRGID']
    self.obsid = hdulist[0].header['OBSID']
    self.datalab = hdulist[0].header['DATALAB']
    self.telescope = hdulist[0].header['TELESCOP']
    self.instrument = hdulist[0].header['INSTRUME']
    datetime_string = hdulist[0].header['DATE-OBS'] +' '+ hdulist[0].header['TIME-OBS']
    self.utdatetime = dateutil.parser.parse(datetime_string)
    self.obstype = hdulist[0].header['OBSTYPE']
    hdulist.close()

