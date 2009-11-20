from FitsStorage import *
from mod_python import apache

def summary(req, progid, obsid, date):
  req.content_type = "text/html"
  req.write("<html>")
  title = "FITS header summary table"
  if(len(progid)>0):
    title += "; Program ID: %s" % (progid)
  if(len(obsid)>0):
    title += "; Observation ID: %s" % (obsid)
  if(len(date)>0):
    title += "; Date: %s" % (date)
  req.write("<head><title>%s</title></head>\n" % (title))
  req.write("<body>")
  req.write("<H1>%s</H1>" % (title))
  webhdrsummary(req, list_headers(progid, obsid, date))
  return apache.OK

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

  # We should order by datetime
  query = query.order_by(Header.utdatetime)

  # Return the list of DiskFile objects
  return query.all()

def webhdrsummary(req, headers):
  # Given a list of header instances and an apache request oject
  # Write a header summary table to the request object
  req.write("<TABLE border=1>")
  req.write("<TR>")
  req.write("<TH>Filename</TH>")
  req.write("<TH>Program ID</TH>")
  req.write("<TH>Observation ID</TH>")
  req.write("<TH>Data Label</TH>")
  req.write("<TH>Instrument</TH>")
  req.write("<TH>Date Time</TH>")
  req.write("<TH>ObsType</TH>")
  req.write("</TR>")
  for h in headers:
    req.write("<TR>")
    req.write("<TD>%s</TD>" % (h.diskfile.file.filename))
    req.write("<TD>%s</TD>" % (h.progid))
    req.write("<TD>%s</TD>" % (h.obsid))
    req.write("<TD>%s</TD>" % (h.datalab))
    req.write("<TD>%s</TD>" % (h.instrument))
    req.write("<TD>%s</TD>" % (h.utdatetime))
    req.write("<TD>%s</TD>" % (h.obstype))
    req.write("</TR>\n")
  req.write("</TABLE>\n")
