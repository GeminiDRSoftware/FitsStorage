from FitsStorage import *
from FitsStorageUtils import *
from GeminiMetadataUtils import *
from mod_python import apache

# This is the main header summary generator
def summary(req, type, selection, orderby):
  req.content_type = "text/html"
  req.write("<html>")
  title = "FITS header %s table" % (type)
  if('progid' in selection):
    title += "; Program ID: %s" % (selection['progid'])
  if('obsid' in selection):
    title += "; Observation ID: %s" % (selection['obsid'])
  if('date' in selection):
    title += "; Date: %s" % (selection['date'])
  if('inst' in selection):
    title += "; Instrument: %s" % (selection['inst'])
  req.write("<head>")
  req.write("<title>%s</title>" % (title))
  req.write('<link rel="stylesheet" href="/htmldocs/table.css">')
  req.write("</head>\n")
  req.write("<body>")
  if (fits_system_status == "development"):
    req.write('<h1>This is the development system, please use <a href="http://fits/">fits</a> for operational use</h1>')
  req.write("<H1>%s</H1>" % (title))

  # If this is a diskfiles summary, select even ones that are not present
  if(type != 'diskfiles'):
    # Usually, we want to only select headers with diskfiles that are present
    selection['present']=True

  webhdrsummary(req, type, list_headers(selection, orderby))
  req.write("</body></html>")
  return apache.OK

def list_headers(selection, orderby):
  # The basic query...
  query = session.query(Header).select_from(join(Header, join(DiskFile, File)))

  # Do want to select Header object for which diskfile.present is true?
  if('present' in selection):
    query = query.filter(DiskFile.present == selection['present'])

  # Is this a completely open query?
  openquery=1

  # Should we query by obsid?
  if('obsid' in selection):
    query = query.filter(Header.obsid==selection['obsid'])
    openquery=0

  # Should we query by progid?
  if('progid' in selection):
    query = query.filter(Header.progid==selection['progid'])
    openquery=0

  # Should we query by date?
  if('date' in selection):
    # Parse the date to start and end datetime objects
    startdt = dateutil.parser.parse("%s 00:00:00" % (selection['date']))
    oneday = datetime.timedelta(days=1)
    enddt = startdt + oneday
    # check it's between these two
    query = query.filter(Header.utdatetime >= startdt).filter(Header.utdatetime <= enddt)
    openquery=0

  # Should we query by instrument?
  if('inst' in selection):
    query = query.filter(Header.instrument==selection['inst'])
    # do not alter openquery here.

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
      if((orderby[i] == 'filename') or (orderby[i] == 'filename_asc')):
        query = query.order_by(File.filename)
      if(orderby[i] == 'filename_desc'):
        query = query.order_by(desc(File.filename))
      if((orderby[i] == 'filter') or (orderby[i] == 'filter_asc')):
        query = query.order_by(Header.filter)
      if(orderby[i] == 'filter_desc'):
        query = query.order_by(desc(Header.filter))
      if((orderby[i] == 'exptime') or (orderby[i] == 'exptime_asc')):
        query = query.order_by(Header.exptime)
      if(orderby[i] == 'exptime_desc'):
        query = query.order_by(desc(Header.exptime))

  # If this is an open query, we should reverse sort by date-time
  if(openquery):
    query = query.order_by(desc(Header.utdatetime))
  else:
    # By default we should order by filename
    query = query.order_by(File.filename)

  # If this is an open query, we should limit to 2500 responses
  if(openquery):
    query = query.limit(2500)

  # Return the list of DiskFile objects
  return query.all()

def webhdrsummary(req, type, headers):
  # Given an apache request object, summary type and list of header instances
  # Write a header summary table of the appropriate type to the request object
  # Get the uri to use for the re-sort links
  myuri = req.uri

  # A certain amount of parsing the summary type...
  want=[]
  if(type == 'summary'):
    want.append('obs')
    want.append('qa')
  if(type == 'diskfiles'):
    want.append('diskfiles')

  # Output the start of the table including column headings
  # First part included in all summary types
  req.write('<TABLE border=0>')
  req.write('<TR class=tr_head>')
  req.write('<TH>Filename <a href="%s?orderby=filename_asc">&uarr</a><a href="%s?orderby=filename_desc">&darr</a></TH>' % (myuri, myuri))
  req.write('<TH>Data Label <a href="%s?orderby=datalab_asc">&uarr</a><a href="%s?orderby=datalab_desc">&darr</a></TH>' % (myuri, myuri))
  req.write('<TH>UT Date Time <a href="%s?orderby=utdatetime_asc">&uarr</a><a href="%s?orderby=utdatetime_desc">&darr</a></TH>' % (myuri, myuri))
  req.write('<TH><abbr title="Instrument">Inst</abbr> <a href="%s?orderby=instrument_asc">&uarr</a><a href="%s?orderby=instrument_desc">&darr</a></TH>' % (myuri, myuri))
  
  # This is the 'obs' part 
  if('obs' in want):
    req.write('<TH><abbr title="ObsClass">Class</abbr> <a href="%s?orderby=obsclass_asc">&uarr</a><a href="%s?orderby=obsclass_desc">&darr</a></TH>' % (myuri, myuri))
    req.write('<TH><abbr title="ObsType">Type</abbr> <a href="%s?orderby=obstype_asc">&uarr</a><a href="%s?orderby=obstype_desc">&darr</a></TH>' % (myuri, myuri))
    req.write('<TH>Filter <a href="%s?orderby=filter_asc">&uarr</a><a href="%s?orderby=filter_desc">&darr</a></TH>' % (myuri, myuri))
    req.write('<TH><abbr title="Exposure Time">ExpTime</abbr> <a href="%s?orderby=exptime_asc">&uarr</a><a href="%s?orderby=exptime_desc">&darr</a>' % (myuri, myuri))
    req.write('<TH><abbr title="AirMass">AM</abbr> <a href="%s?orderby=airmass_asc">&uarr</a><a href="%s?orderby=airmass_desc">&darr</a></TH>' % (myuri, myuri))
    req.write('<TH><abbr title="Localtime">Lcltime</abbr> <a href="%s?orderby=localtime_asc">&uarr</a><a href="%s?orderby=localtime_desc">&darr</a></TH>' % (myuri, myuri))

  # This is the 'qa' part
  if('qa' in want):
    req.write('<TH><abbr title="QA State">QA</abbr> <a href="%s?orderby=qastate_asc">&uarr</a><a href="%s?orderby=qastate_desc">&darr</a></TH>' % (myuri, myuri))
    req.write('<TH><abbr title="Raw IQ">IQ</abbr> <a href="%s?orderby=rawiq_asc">&uarr</a><a href="%s?orderby=rawiq_desc">&darr</a></TH>' % (myuri, myuri))
    req.write('<TH><abbr title="Raw CC">CC</abbr> <a href="%s?orderby=rawcc_asc">&uarr</a><a href="%s?orderby=rawcc_desc">&darr</a></TH>' % (myuri, myuri))
    req.write('<TH><abbr title="Raw WV">WV</abbr> <a href="%s?orderby=rawwv_asc">&uarr</a><a href="%s?orderby=rawwv_desc">&darr</a></TH>' % (myuri, myuri))
    req.write('<TH><abbr title="Raw BG">BG</abbr> <a href="%s?orderby=rawbg_asc">&uarr</a><a href="%s?orderby=rawbg_desc">&darr</a></TH>' % (myuri, myuri))

  # This is the 'diskfiles' part
  if('diskfiles' in want):
    req.write('<TH>Present</TH>')
    req.write('<TH>Entry</TH>')
    req.write('<TH>Lastmod</TH>')
    req.write('<TH>Size</TH>')
    req.write('<TH>CCRC</TH>')

  # Last bit included in all summary types
  req.write('</TR>')

  # Loop through the header list, outputing table rows
  even=0
  for h in headers:
    even = not even
    if(even):
      cs = "tr_even"
    else:
      cs = "tr_odd"
    # Again, the first part included in all summary types
    req.write("<TR class=%s>" % (cs))

    # Parse the datalabel first
    dl = GeminiDataLabel(h.datalab)

    # The filename cell, with the link to the full headers and the optional WMD and FITS error flags
    if(h.diskfile.fverrors):
      fve='<a href="/fitsverify/%d">- fits!</a>' % (h.diskfile.id)
    else:
      fve=''
    # Do not raise the WMD flag on ENG data
    iseng = bool(dl.datalabel) and dl.project.iseng
    if((not iseng) and (not h.diskfile.wmdready)):
      wmd='<a href="/wmdreport/%d">- md!</a>' % (h.diskfile.id)
    else:
      wmd=''
    req.write('<TD><A HREF="/fullheader/%s">%s</A> %s %s</TD>' % (h.diskfile.file.filename, h.diskfile.file.filename, fve, wmd))

    # The datalabel, parsed to link to the programid and obsid,
    if(dl.datalabel):
      req.write('<TD><NOBR><a href="/summary/%s">%s</a>-<a href="/summary/%s">%s</a>-%s</NOBR></TD>' % (dl.projectid, dl.projectid, dl.obsid, dl.obsnum, dl.dlnum))
    else:
      req.write('<TD>%s</TD>' % h.datalab)

    if(h.utdatetime):
      req.write("<TD><NORB>%s</NOBR></TD>" % (h.utdatetime.strftime("%Y-%m-%d %H:%M:%S")))
    else:
      req.write("<TD>%s</TD>" % ("None"))
    req.write("<TD>%s</TD>" % (h.instrument))

    # Now the 'obs' part
    if('obs' in want):
      req.write("<TD>%s</TD>" % (h.obsclass))
      req.write("<TD>%s</TD>" % (h.obstype))
      req.write("<TD>%s</TD>" % (h.filter))
      req.write("<TD>%s</TD>" % (h.exptime))
      req.write("<TD>%s</TD>" % (h.airmass))
      if(h.localtime):
        req.write("<TD>%s</TD>" % (h.localtime.strftime("%H:%M:%S")))
      else:
        req.write("<TD>%s</TD>" % ("None"))

    # Now the 'qa' part
    # Abreviate the raw XX values to 4 characters
    if('qa' in want):
      req.write('<TD>%s</TD>' % (h.qastate))

      if(h.rawiq and percentilecre.match(h.rawiq)):
        req.write('<TD><abbr title="%s">%s</abbr></TD>' % (h.rawiq, h.rawiq[0:4]))
      else:
        req.write('<TD>%s</TD>' % (h.rawiq))

      if(h.rawcc and percentilecre.match(h.rawcc)):
        req.write('<TD><abbr title="%s">%s</abbr></TD>' % (h.rawcc, h.rawcc[0:4]))
      else:
        req.write('<TD>%s</TD>' % (h.rawcc))

      if(h.rawwv and percentilecre.match(h.rawwv)):
        req.write('<TD><abbr title="%s">%s</abbr></TD>' % (h.rawwv, h.rawwv[0:4]))
      else:
        req.write('<TD>%s</TD>' % (h.rawwv))
 
      if(h.rawbg and percentilecre.match(h.rawbg)):
        req.write('<TD><abbr title="%s">%s</abbr></TD>' % (h.rawbg, h.rawbg[0:4]))
      else:
        req.write('<TD>%s</TD>' % (h.rawbg))

    # the 'diskfiles' part
    if('diskfiles' in want):
      req.write("<TD>%s</TD>" % (h.diskfile.present))
      req.write("<TD>%s</TD>" % (h.diskfile.entrytime))
      req.write("<TD>%s</TD>" % (h.diskfile.lastmod))
      req.write("<TD>%s</TD>" % (h.diskfile.size))
      req.write("<TD>%s</TD>" % (h.diskfile.ccrc))

    # And again last bit included in all summary types
    req.write("</TR>\n")
  req.write("</TABLE>\n")

# this lists programs observed on a given night
def progsobserved(req, things):
  if(things):
    arg = things.pop(0)
  else:
    arg='today'
  if(gemini_date(arg)):
    date=gemini_date(arg)
  else:
    date=gemini_date('today')

  # Parse the date to start and end datetime objects
  startdt = dateutil.parser.parse("%s 00:00:00" % (date))
  oneday = datetime.timedelta(days=1)
  enddt = startdt + oneday
  # create a query for between these two
  query = session.query(Header.progid).filter(Header.utdatetime >= startdt).filter(Header.utdatetime <= enddt).group_by(Header.progid)
  list = query.all()
  req.content_type = "text/html"
  req.write('<html><head><title>Programs Observed on %s</title></head><body><h1>Programs Observed on %s</h1><p>' % (date, date))
  for row in list:
    p = row[0]
    if(p):
      req.write('%s ' % p)
  req.write('</p></body></html>')
  return apache.OK

