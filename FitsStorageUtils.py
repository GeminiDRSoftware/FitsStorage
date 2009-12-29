from FitsStorage import *
from sqlalchemy.orm.exc import NoResultFound


# Compile regular expresisons here
crefits = re.compile("\S*.fits$")

def create_tables(session):
  # Create the tables
  File.metadata.create_all(bind=pg_db)
  DiskFile.metadata.create_all(bind=pg_db)
  Header.metadata.create_all(bind=pg_db)
  IngestQueue.metadata.create_all(bind=pg_db)

  # Now grant the apache user select on them for the www queries
  session.execute("GRANT SELECT ON file, diskfile, header TO apache");

def fitsfilename(filename):
  # Takes a filename with optional .fits ending and returns it
  # ensuring that it ends in .fits
  match = crefits.match(filename)
  if(not match):
    filename = "%s.fits" % filename
  return filename

def ingest_file(session, filename, path, force_crc, skip_fv, skip_wmd):
  # Make a file instance
  file = File(filename, path)

  # First check if the file exists
  if(not(file.exists())):
    print "cannot access ", file.fullpath
    return

  # Check if this filename is already in the database

  query = session.query(File).filter(File.filename==file.filename).filter(File.path==file.path)
  if(query.first()):
    #print "Already in file table"
    # This will throw an error if there is more than one entry
    # There's a way to handle that nicely internally if we want.
    file = query.one()
  else:
    print "Adding new file table entry"
    file = File(filename, path)
    session.add(file)
    session.commit();

  # See if a diskfile for this file already exists and is present
  query = session.query(DiskFile).filter(DiskFile.file_id==file.id).filter(DiskFile.present==True)
  if(query.first()):
    # Yes, it's already there.
    #print "already present in diskfile table..."
    # Ensure there's only one and get an instance of it
    diskfile = query.one()
    # Has the file changed since we last recorded it?
    # By default check lastmod time first
    # there is a subelty wrt timezones here.
    if((diskfile.lastmod.replace(tzinfo=None) != diskfile.file.lastmod()) or force_crc):
      #print "lastmod time indicates file modification"
      # Check the CRC to be sure if it's changed
      if(diskfile.ccrc == diskfile.file.ccrc()):
        #print "crc indicates no change"
        add_diskfile=0
      else:
        print "crc indicates file has changed - reingesting"
        # Set the present flag on the current one to false and create a new entry
        diskfile.present=False
        add_diskfile=1
    else:
      #print "lastmod time indicates file unchanged, not checking further"
      add_diskfile=0

  else:
    # No not present, insert into diskfile table
    #print "No DiskFile exists"
    add_diskfile=1
  
  if(add_diskfile):
    print "Adding new DiskFile entry"
    diskfile = DiskFile(file, skip_fv, skip_wmd)
    session.add(diskfile)
    session.commit()
    print "Adding new Header entry"
    header = Header(diskfile)
    session.add(header)
    session.commit()
  
  session.commit();

def pop_ingestqueue(session):
  # Return the next thing to ingest off the ingest queue
  # Next is defined by a sort on the filename to get most recent first
  # ... and not inprogress
  # Set the inprogress column to true
  # Must do this atomically, or at least check for race condition
  # Also, when we go inprogress on an entry in the queue, we should delete all other entries for the same filename

  # Ensure nothing outstanding
  session.flush()

  # Form the query, with for_update which adds FOR UPDATE to the SQL query. The resulting lock ends when the transaction ends
  query=session.query(IngestQueue).with_lockmode('update').filter(IngestQueue.inprogress == False).order_by(desc(IngestQueue.filename))

  # Try and get a value. If we fail, there are none, so bail out
  try:
    iq = query.first()
  except NoResultFound:
    # Not that anything has been done yet, but rollback anyway to clear the transaction...
    session.rollback()
    session.close()
    iq=''

  if(iq):
    iq.inprogress=True

    # Find other instances
    others = session.query(IngestQueue).filter(IngestQueue.inprogress == False).filter(IngestQueue.filename==iq.filename).all()
    for o in others:
      session.delete(o)

  # And we're done
  session.commit()
  return iq
  
def addto_ingestqueue(session, filename, path):
  # Adds to the ingestqueu
  iq = IngestQueue(filename, path)
  session.add(iq)
  session.commit()

def ingestqueue_length(session):
  # return the length of the ingest queue
  length = session.query(IngestQueue).filter(IngestQueue.inprogress == False).count()
  return length
