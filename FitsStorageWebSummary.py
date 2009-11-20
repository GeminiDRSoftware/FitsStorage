from FitsStorage import *

def list_headers(progid, obsid, date):
  # We want to select Header object for which diskfile.present is true
  query = session.query(Header).select_from(join(Header, DiskFile)).filter(DiskFile.present == True)

  # Should we query by obsid?
  if(len(obsid)>0):
    query = query.filter(Header.obsid==obsid)

  # Should we query by progid?
  if(len(progid)>0):
    query = query.filter(Header.progid==progid)

  # Should we query by date?
  #if(len(date)>0):
    # Parse the date to a datetime object
    # make two datetimes one at start and end of date
    # check it's between these two

  # Return the list of DiskFile objects
  return query.all()

def webhdrsummary(req, headers):
  # Given a list of header instances and an apache request oject
  # Write a header summary to the request object
  for h in headers:
    req.write("Filename: %s\n" % (h.diskfile.file.filename))
    req.write("-- Progid: %s\n" % (h.progid))
    req.write("-- Obsid: %s\n" % (h.obsid))
    req.write("-- DataLabel: %s\n" % (h.datalab))
    req.write("\n")

