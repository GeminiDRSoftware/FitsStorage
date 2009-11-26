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

def summary(req, progid, obsid, date, inst, args):
  req.content_type = "text/html"
  req.write("<html>")
  title = "FITS header summary table"
  if(len(progid)>0):
    title += "; Program ID: %s" % (progid)
  if(len(obsid)>0):
    title += "; Observation ID: %s" % (obsid)
  if(len(date)>0):
    title += "; Date: %s" % (date)
  if(len(inst)>0):
    title += "; Instrument: %s" % (inst)
  req.write("<head>")
  req.write("<title>%s</title>" % (title))
  req.write('<link rel="stylesheet" href="/htmldocs/table.css">')
  req.write("</head>\n")
  req.write("<body>")
  req.write("<H1>%s</H1>" % (title))
  webhdrsummary(req, list_headers(progid, obsid, date, inst, args))
  req.write("</body></html>")
  return apache.OK

def list_headers(progid, obsid, date, inst, args):
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

  # Should we query by instrument?
  if(len(inst)>0):
    query = query.filter(Header.instrument==inst)
    # do not alter openquery here.

  # Do we have any valid orderby arguments?
  # we should validate more here
  orderby=[]
  for i in range(len(args)):
    match=re.match('orderby\=(\S*)', args[i])
    if(match):
      thing=match.group(1)
      orderby.append(thing)

  # Do we have any order by arguments?
  if(orderby):
    # Yes, apply them to the query
    for i in range(len(orderby)):
      if((orderby[i] == 'instrument') or (orderby[i] == 'instrument_asc')):
        query = query.order_by(Header.instrument)
      if(orderby[i] == 'instrument_desc'):
        query = query.order_by(desc(Header.instrument))
      if((orderby[i] == 'datalab') or (orderby[i] == 'datalab_asc')):
        query = query.order_by(Header.datalab)
      if(orderby[i] == 'datalab_desc'):
        query = query.order_by(desc(Header.datalab))
      if((orderby[i] == 'obsclass') or (orderby[i] == 'obsclass_asc')):
        query = query.order_by(Header.obsclass)
      if(orderby[i] == 'obsclass_desc'):
        query = query.order_by(desc(Header.obsclass))
      if((orderby[i] == 'airmass') or (orderby[i] == 'airmass_asc')):
        query = query.order_by(Header.airmass)
      if(orderby[i] == 'airmass_desc'):
        query = query.order_by(desc(Header.airmass))
      if((orderby[i] == 'utdatetime') or (orderby[i] == 'utdatetime_asc')):
        query = query.order_by(Header.utdatetime)
      if(orderby[i] == 'obstype_desc'):
        query = query.order_by(desc(Header.obstype))
      if((orderby[i] == 'utdatetime') or (orderby[i] == 'utdatetime_asc')):
        query = query.order_by(Header.utdatetime)
      if(orderby[i] == 'utdatetime_desc'):
        query = query.order_by(desc(Header.utdatetime))
      if((orderby[i] == 'localtime') or (orderby[i] == 'localtime_asc')):
        query = query.order_by(Header.localtime)
      if(orderby[i] == 'localtime_desc'):
        query = query.order_by(desc(Header.localtime))
      if((orderby[i] == 'rawiq') or (orderby[i] == 'rawiq_asc')):
        query = query.order_by(Header.rawiq)
      if(orderby[i] == 'rawiq_desc'):
        query = query.order_by(desc(Header.rawiq))
      if((orderby[i] == 'rawcc') or (orderby[i] == 'rawcc_asc')):
        query = query.order_by(Header.rawcc)
      if(orderby[i] == 'rawcc_desc'):
        query = query.order_by(desc(Header.rawcc))
      if((orderby[i] == 'rawbg') or (orderby[i] == 'rawbg_asc')):
        query = query.order_by(Header.rawbg)
      if(orderby[i] == 'rawbg_desc'):
        query = query.order_by(desc(Header.rawbg))
      if((orderby[i] == 'rawwv') or (orderby[i] == 'rawwv_asc')):
        query = query.order_by(Header.rawwv)
      if(orderby[i] == 'rawwv_desc'):
        query = query.order_by(desc(Header.rawwv))
      if((orderby[i] == 'qastate') or (orderby[i] == 'qastate_asc')):
        query = query.order_by(Header.qastate)
      if(orderby[i] == 'qastate_desc'):
        query = query.order_by(desc(Header.qastate))

  # If this is an open query, we should reverse sort by date-time
  if(openquery):
    query = query.order_by(desc(Header.utdatetime))
  else:
    # We should order by datetime
    query = query.order_by(Header.utdatetime)

  # If this is an open query, we should limit to 2500 responses
  if(openquery):
    query = query.limit(2500)

  # Return the list of DiskFile objects
  return query.all()

def webhdrsummary(req, headers):
  # Given a list of header instances and an apache request oject
  # Write a header summary table to the request object
  # Get the uri to use for the re-sort links
  myuri = req.uri
  req.write('<TABLE border=0>')
  req.write('<TR class=tr_head>')
  req.write('<TH>Filename</TH>')
  req.write('<TH>Data Label <a href="%s?orderby=datalab_asc">&uarr</a><a href="%s?orderby=datalab_desc">&darr</a></TH>')
  req.write('<TH>Instrument <a href="%s?orderby=instrument_asc">&uarr</a><a href="%s?orderby=instrument_desc">&darr</a></TH>' % (myuri, myuri))
  req.write('<TH>ObsClass <a href="%s?orderby=obsclass_asc">&uarr</a><a href="%s?orderby=obsclass_desc">&darr</a></TH>' % (myuri, myuri))
  req.write('<TH>ObsType <a href="%s?orderby=obstype_asc">&uarr</a><a href="%s?orderby=obstype_desc">&darr</a></TH>' % (myuri, myuri))
  req.write('<TH>Airmass <a href="%s?orderby=airmass_asc">&uarr</a><a href="%s?orderby=airmass_desc">&darr</a></TH>' % (myuri, myuri))
  req.write('<TH>UT Date Time <a href="%s?orderby=utdatetime_asc">&uarr</a><a href="%s?orderby=utdatetime_desc">&darr</a></TH>' % (myuri, myuri))
  req.write('<TH>Lcltime <a href="%s?orderby=localtime_asc">&uarr</a><a href="%s?orderby=localtime_desc">&darr</a></TH>' % (myuri, myuri))
  req.write('<TH>QA State <a href="%s?orderby=qastate_asc">&uarr</a><a href="%s?orderby=qastate_desc">&darr</a></TH>')
  req.write('<TH>Raw IQ <a href="%s?orderby=rawiq_asc">&uarr</a><a href="%s?orderby=rawiq_desc">&darr</a></TH>' % (myuri, myuri))
  req.write('<TH>Raw CC <a href="%s?orderby=rawcc_asc">&uarr</a><a href="%s?orderby=rawcc_desc">&darr</a></TH>' % (myuri, myuri))
  req.write('<TH>Raw WV <a href="%s?orderby=rawwv_asc">&uarr</a><a href="%s?orderby=rawwv_desc">&darr</a></TH>' % (myuri, myuri))
  req.write('<TH>Raw BG <a href="%s?orderby=rawbg_asc">&uarr</a><a href="%s?orderby=rawbg_desc">&darr</a></TH>' % (myuri, myuri))
  req.write('</TR>')
  even=0
  for h in headers:
    even = not even
    if(even):
      cs = "tr_even"
    else:
      cs = "tr_odd"
    req.write("<TR class=%s>" % (cs))
    if(h.diskfile.fverrors):
      fve='<a href="/fvreport/%d"> - !FITS!</a>' % (h.diskfile.id)
    else:
      fve=''
    req.write('<TD><A HREF="/fullheader/%s">%s</A>%s</TD>' % (h.diskfile.file.filename, h.diskfile.file.filename, fve))
    req.write("<TD>%s</TD>" % (h.datalab))
    req.write("<TD>%s</TD>" % (h.instrument))
    req.write("<TD>%s</TD>" % (h.obsclass))
    req.write("<TD>%s</TD>" % (h.obstype))
    req.write("<TD>%s</TD>" % (h.airmass))
    if(h.utdatetime):
      req.write("<TD>%s</TD>" % (h.utdatetime.strftime("%Y-%m-%d %H:%M:%S")))
    else:
      req.write("<TD>%s</TD>" % ("None"))
    if(h.localtime):
      req.write("<TD>%s</TD>" % (h.localtime.strftime("%H:%M:%S")))
    else:
      req.write("<TD>%s</TD>" % ("None"))
    req.write("<TD>%s</TD>" % (h.qastate))
    req.write("<TD>%s</TD>" % (h.rawiq))
    req.write("<TD>%s</TD>" % (h.rawcc))
    req.write("<TD>%s</TD>" % (h.rawwv))
    req.write("<TD>%s</TD>" % (h.rawbg))
    req.write("</TR>\n")
  req.write("</TABLE>\n")
