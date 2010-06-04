"""
This module provides various utility functions for the Fits Storage
System.
"""
from FitsStorage import *
from FitsStorageLogger import logger
from sqlalchemy.orm.exc import NoResultFound, ObjectDeletedError

def create_tables(session):
  """
  Creates the database tables and grants the apache user
  SELECT on the appropriate ones
  """
  # Create the tables
  File.metadata.create_all(bind=pg_db)

  # Now grant the apache user select on them for the www queries
  session.execute("GRANT SELECT ON file, diskfile, header, gmos, niri, tape, tape_id_seq, tapewrite, tapefile TO apache");
  session.execute("GRANT INSERT,UPDATE ON tape, tape_id_seq TO apache");
  session.commit()

def ingest_file(session, filename, path, force_crc, skip_fv, skip_wmd):
  """
  Ingests a file into the databse. If the file isn't known to the database
  at all, all three (file, diskfile, header) table entries are created.
  If the file is already in the database but has been modified, the
  existing diskfile entry is marked as not present and nee diskfile
  and header entries are created. If the file is in the database and
  has not been modified since it was last ingested, then this function
  does not modify the database.

  session: the sqlalchemy database session to use
  filename: the filename of the file to ingest
  path: the path to the file to ingest
  force_crc: normally this function will compare the last modified
             timestamp on the file to that of the record of the file
             in the database to determine if it has possibly changed,
             and only checks the CRC if it has possibly changed. Setting
             this parameter to true forces a CRC comparison irrespective
             of the last modification timestamps.
  skip_fv: causes the ingest to skip running fitsverify on the file
  skip_wmd: causes the ingest to skip running wmd on the file.
  """
  logger.debug("ingest_file %s" % filename)
  # Make a file instance
  file = File(filename, path)

  # First check if the file exists
  if(not(file.exists())):
    logger.debug("cannot access %s", file.fullpath())
    check_present(session, filename)
    return

  # Check if this filename is already in the database

  query = session.query(File).filter(File.filename==file.filename).filter(File.path==file.path)
  if(query.first()):
    logger.debug("Already in file table")
    # This will throw an error if there is more than one entry
    file = query.one()
  else:
    logger.debug("Adding new file table entry")
    file = File(filename, path)
    session.add(file)
    session.commit();

  # See if a diskfile for this file already exists and is present
  query = session.query(DiskFile).filter(DiskFile.file_id==file.id).filter(DiskFile.present==True)
  if(query.first()):
    # Yes, it's already there.
    logger.debug("already present in diskfile table...")
    # Ensure there's only one and get an instance of it
    diskfile = query.one()
    # Has the file changed since we last recorded it?
    # By default check lastmod time first
    # there is a subelty wrt timezones here.
    if((diskfile.lastmod.replace(tzinfo=None) != diskfile.file.lastmod()) or force_crc):
      logger.debug("lastmod time indicates file modification")
      # Check the CRC to be sure if it's changed
      if(diskfile.ccrc == diskfile.file.ccrc()):
        logger.debug("crc indicates no change")
        add_diskfile=0
      else:
        logger.debug("crc indicates file has changed - reingesting")
        # Set the present and canonical flags on the current one to false and create a new entry
        diskfile.present=False
        diskfile.canonical=False
        add_diskfile=1
    else:
      logger.debug("lastmod time indicates file unchanged, not checking further")
      add_diskfile=0

  else:
    # No not present, insert into diskfile table
    logger.debug("No DiskFile exists")
    add_diskfile=1
  
  if(add_diskfile):
    logger.debug("Adding new DiskFile entry")
    diskfile = DiskFile(file, skip_fv, skip_wmd)
    session.add(diskfile)
    session.commit()
    logger.debug("Adding new Header entry")
    header = Header(diskfile)
    session.add(header)
    inst = header.instrument
    logger.debug("Instrument is: %s" % inst)
    session.commit()
    # Add the instrument specific tables
    if(inst=='GMOS-N' or inst=='GMOS-S'):
      logger.debug("Adding new GMOS entry")
      gmos = Gmos(header)
      session.add(gmos)
      session.commit()
    if(inst=='NIRI'):
      logger.debug("Adding new NIRI entry")
      niri = Niri(header)
      session.add(niri)
      session.commit()
  
  session.commit();

def check_present(session, filename):
  """
  Check to see if the named file is present in the database and
  marked as present in the diskfile table.
  If so, checks to see if it's actually on disk and if not
  marks it as not present in the diskfile table
  """

  # Search for file
  query = session.query(File).filter(File.filename == filename)
  if(query.first()):
    logger.debug("%s is present in file table", filename)
    file = query.one()
    # OK, is there a diskfile that's present for it
    query = session.query(DiskFile).filter(DiskFile.file_id==file.id).filter(DiskFile.present==True)
    if(query.first()):
      diskfile = query.one()
      logger.debug("%s is present=True in diskfile table at diskfile_id = %s" % (filename, diskfile.id))
      # Is the file actually present on disk?
      if(file.exists()):
        logger.debug("%s is actually present on disk. That's good" % filename)
      else:
        logger.info("%s is present in diskfile table id %d but missing on the disk." % (filename, diskfile.id))
        logger.info("Marking diskfile id %d as not present" % diskfile.id)
        diskfile.present=False

def pop_ingestqueue(session):
  """
  Returns the next thing to ingest off the ingest queue, and sets the
  inprogress flag on that entry.

  The select and update inprogress are done with a transaction lock
  to avoid race conditions or duplications when there is more than 
  one process processing the ingest queue.

  Next to ingest is defined by a sort on the filename to get the
  newest filename that is not already inprogress.

  Also, when we go inprogress on an entry in the queue, we 
  delete all other entries for the same filename.
  """

  # Ensure nothing outstanding
  session.flush()

  # Form the query, with for_update which adds FOR UPDATE to the SQL query. The resulting lock ends when the transaction ends
  query=session.query(IngestQueue).with_lockmode('update').filter(IngestQueue.inprogress == False).order_by(desc(IngestQueue.filename))

  # Try and get a value. If we fail, there are none, so bail out
  try:
    iq = query.first()
  except NoResultFound:
    logger.debug("No item to pop on ingestqueue")
    iq=None

  if(iq):
    iq.inprogress=True

    # Find other instances
    others = session.query(IngestQueue).filter(IngestQueue.inprogress == False).filter(IngestQueue.filename==iq.filename).all()
    for o in others:
      session.delete(o)

  # And we're done
  session.commit()
  if(iq):
    logger.debug("Popped id %d from ingestqueue" % iq.id)
  else:
    logger.debug("Popped null entry from ingest queue")
  return iq
  
def addto_ingestqueue(session, filename, path):
  """
  Adds a file to the ingest queue
  """
  iq = IngestQueue(filename, path)
  session.add(iq)
  session.commit()
  try:
    logger.debug("Added id %d for filename %s to ingestqueue" % (iq.id, iq.filename))
    return iq.id
  except ObjectDeletedError:
    logger.debug("Added filename %s to ingestqueue which was immediately deleted" % filename)

def ingestqueue_length(session):
  """
  return the length of the ingest queue
  """
  length = session.query(IngestQueue).filter(IngestQueue.inprogress == False).count()
  return length

def ingest_standards(session, filename):
  """
  Load the standards text file into the Standards table
  """
  # open the standards text file
  f = open(filename, 'r')
  list = f.readlines()
  f.close()

  # Loop through entries, adding to table
  for line in list:
    if(line[0]!='#'):
      fields = line.split()

      # Create and populate a standard instance
      std = PhotStandard()
      try:
        std.name = fields[0]
        std.field = fields[1]
        std.ra = float(fields[2])
        std.dec = float(fields[3])
        if(fields[4]!='None'):
          std.u_mag = float(fields[4])
        if(fields[5]!='None'):
          std.v_mag = float(fields[5])
        if(fields[6]!='None'):
          std.g_mag = float(fields[6])
        if(fields[7]!='None'):
          std.r_mag = float(fields[7])
        if(fields[8]!='None'):
          std.i_mag = float(fields[8])
        if(fields[9]!='None'):
          std.z_mag = float(fields[9])
        if(fields[10]!='None'):
          std.y_mag = float(fields[10])
        if(fields[11]!='None'):
          std.j_mag = float(fields[11])
        if(fields[12]!='None'):
          std.h_mag = float(fields[12])
        if(fields[13]!='None'):
          std.k_mag = float(fields[13])
        if(fields[14]!='None'):
          std.lprime_mag = float(fields[14])
        if(fields[15]!='None'):
          std.m_mag = float(fields[15])
      except ValueError:
        print "Fields: %s" % str(fields)
        raise
    
      # Add to database session
      session.add(std)
      session.commit()
