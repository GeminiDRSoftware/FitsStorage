from FitsStorage import *
from mod_python import apache

fitscre = re.compile('\S*.fits$')

def fullheader(req, filename):
  # If the filename is missing the .fits, then add it
  match = fitscre.match(filename)
  if(not match):
    filename = "%s.fits" % (filename)
  
  # First search for a file object with the given filename
  query = session.query(File).filter(File.filename == filename)
  if(query.count()==0):
    req.content_type="text/plain"
    req.write("Cannot find file for: %s\n" % filename)
    return apache.OK

  file = query.one()
  hdulist = pyfits.open(file.fullpath())
  req.write("FITS File: %s (%s)\n\n" % (filename, file.fullpath()))

  for i in range(len(hdulist)):
    req.write("\n--- HDU %s ---\n" % (i))
    req.write(str(hdulist[i].header.ascardlist()))
    req.write('\n')
  hdulist.close()
  return apache.OK

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
  req.write("<head>")
  req.write("<title>%s</title>" % (title))
  req.write('<link rel="stylesheet" href="/htmldocs/table.css">')
  req.write("</head>\n")
  req.write("<body>")
  req.write("<H1>%s</H1>" % (title))
  webhdrsummary(req, list_headers(progid, obsid, date))
  return apache.OK

def list_headers(progid, obsid, date):
  # We want to select Header object for which diskfile.present is true
  query = session.query(Header).select_from(join(Header, DiskFile)).filter(DiskFile.present == True)

  # Is this a completely open query?
  openquery=1

  # Should we query by obsid?
  if(len(obsid)>0):
    query = query.filter(Header.obsid==obsid)
    openquery=0

  # Should we query by progid?
  if(len(progid)>0):
    query = query.filter(Header.progid==progid)
    openquery=0

  # Should we query by date?
  if(len(date)>0):
    # Parse the date to start and end datetime objects
    startdt = dateutil.parser.parse("%s 00:00:00" % (date))
    oneday = datetime.timedelta(days=1)
    enddt = startdt + oneday
    # check it's between these two
    query = query.filter(Header.utdatetime >= startdt).filter(Header.utdatetime <= enddt)
    openquery=0

  # If this is a completely open query, we should reverse sort by date-time
  # and limit the number of responses to say 2500
  if(openquery):
    query = query.order_by(desc(Header.utdatetime)).limit(2500)
  else:
    # We should order by datetime
    query = query.order_by(Header.utdatetime)

  # Return the list of DiskFile objects
  return query.all()

def webhdrsummary(req, headers):
  # Given a list of header instances and an apache request oject
  # Write a header summary table to the request object
  req.write("<TABLE border=0>")
  req.write("<TR class=tr_head>")
  req.write("<TH>Filename</TH>")
  req.write("<TH>FV E - W</TH>")
  req.write("<TH>Data Label</TH>")
  req.write("<TH>Instrument</TH>")
  req.write("<TH>ObsClass</TH>")
  req.write("<TH>ObsType</TH>")
  req.write("<TH>Date Time</TH>")
  req.write("<TH>QA State</TH>")
  req.write("<TH>Raw IQ</TH>")
  req.write("<TH>Raw CC</TH>")
  req.write("<TH>Raw WV</TH>")
  req.write("<TH>Raw BG</TH>")
  req.write("</TR>")
  even=0
  for h in headers:
    even = not even
    if(even):
      cs = "tr_even"
    else:
      cs = "tr_odd"
    req.write("<TR class=%s>" % (cs))
    req.write('<TD><A HREF="/fullheader/%s">%s</A></TD>' % (h.diskfile.file.filename, h.diskfile.file.filename))
    req.write('<TD><A HREF="/fitsverify/%s">%d - %d</A></TD>' % (h.diskfile.id, h.diskfile.fverrors, h.diskfile.fvwarnings))
    req.write("<TD>%s</TD>" % (h.datalab))
    req.write("<TD>%s</TD>" % (h.instrument))
    req.write("<TD>%s</TD>" % (h.obsclass))
    req.write("<TD>%s</TD>" % (h.obstype))
    if(h.utdatetime):
      req.write("<TD>%s</TD>" % (h.utdatetime.strftime("%Y-%m-%d %H:%M:%S")))
    else:
      req.write("<TD>%s</TD>" % ("None"))
    req.write("<TD>%s</TD>" % (h.rawgemqa))
    req.write("<TD>%s</TD>" % (h.rawiq))
    req.write("<TD>%s</TD>" % (h.rawcc))
    req.write("<TD>%s</TD>" % (h.rawwv))
    req.write("<TD>%s</TD>" % (h.rawbg))
    req.write("</TR>\n")
  req.write("</TABLE>\n")
