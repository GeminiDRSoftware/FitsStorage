"""
This is the Fits Storage Web Summary module. It provides the functions
which query the database and generate html for the web header
summaries.
"""
from FitsStorage import *
from GeminiMetadataUtils import *
from mod_python import apache, util

# Fits filename extension utility
crefits = re.compile("\S*.fits$")
def fitsfilename(filename):
  """
  Takes a filename with optional .fits ending and returns it
  ensuring that it ends in .fits
  """
  match = crefits.match(filename)
  if(not match):
    filename = "%s.fits" % filename
  return filename

def summary(session, req, type, selection, orderby):
  """
  This is the main summary generator.
  session is an sqlalchemy session to access the database
  req is an apache request handler request object
  type is the summary type required
  selection is an array of items to select on, simply passed
    through to the webhdrsummary function
  orderby specifies how to order the output table, simply
    passed through to the webhdrsummary function

  returns an apache request status code

  This function outputs header and footer for the html page,
  and calls the webhdrsummary function to actually generate
  the html table containing the actually summary information.
  """
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
  if('obstype' in selection):
    title += "; ObsType: %s" % (selection['obstype'])
  if('obsclass' in selection):
    title += "; ObsClass: %s" % (selection['obsclass'])

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

  webhdrsummary(session, req, type, list_headers(session, selection, orderby))
  req.write("</body></html>")
  return apache.OK

def list_headers(session, selection, orderby):
  """
  This function queries the database for a list of header table 
  entries that satsify the selection criteria.

  session is an sqlalchemy session on the database
  selection is a dictionary containing fields to select on
  orderby is a list of fields to sort the results by

  Returns a list of Header objects
  """
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

  # Should we query by obstype?
  if('obstype' in selection):
    query = query.filter(Header.obstype==selection['obstype'])
    # do not alter openquery here

  # Should we query by obsclass?
  if('obsclass' in selection):
    query = query.filter(Header.obsclass==selection['obsclass'])
    # do not alter openquery here

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
      if((orderby[i] == 'object') or (orderby[i] == 'object_asc')):
        query = query.order_by(Header.object)
      if(orderby[i] == 'object_desc'):
        query = query.order_by(desc(Header.object))

  # If this is an open query, we should reverse sort by filename
  if(openquery):
    query = query.order_by(desc(File.filename))
  else:
    # By default we should order by filename
    query = query.order_by(File.filename)

  # If this is an open query, we should limit to 2500 responses
  if(openquery):
    query = query.limit(2500)

  # Return the list of DiskFile objects
  return query.all()

def webhdrsummary(session, req, type, headers):
  """
  Generates an HTML header summary table of the specified type from
  the list of header objects provided. Writes that table to an apache
  request object.

  session: sqlalchemy database session
  req: the apache request object to write the output
  type: the summary type required
  headers: the list of header objects to include in the summary
  """
  # Get the uri to use for the re-sort links
  myuri = req.uri

  # A certain amount of parsing the summary type...
  want=[]
  if(type == 'summary'):
    want.append('obs')
    want.append('qa')
    want.append('expamlt')
  if(type == 'ssummary'):
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
    req.write('<TH><abbr title="Object Name">Object</abbr> <a href="%s?orderby=object_asc">&uarr</a><a href="%s?orderby=object_desc">&darr</a></TH>' % (myuri, myuri))
    req.write('<TH><abbr title="Imaging Filter or Spectroscopy Wavelength and Disperser">WaveBand<abbr></TH>')

  # This is the 'expamlt' part - exposure time, airmass, localtime
  if('expamlt' in want):
    req.write('<TH><abbr title="Exposure Time">ExpT</abbr> <a href="%s?orderby=exptime_asc">&uarr</a><a href="%s?orderby=exptime_desc">&darr</a>' % (myuri, myuri))
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

    inst = h.instrument
    if(h.adaptive_optics):
      inst += " + AO"
    req.write("<TD>%s</TD>" % (inst))

    # Now the 'obs' part
    if('obs' in want):
      req.write("<TD>%s</TD>" % (h.obsclass))
      req.write("<TD>%s</TD>" % (h.obstype))
      if (h.object and len(h.object)>12):
        req.write('<TD><abbr title="%s">%s</abbr></TD>' % (h.object, (h.object)[0:12]))
      else:
        req.write("<TD>%s</TD>" % (h.object))

      if(h.spectroscopy):
        try:
          req.write("<TD>%s : %.3f</TD>" % (h.disperser, h.cwave))
        except:
          req.write("<TD>%s : </TD>" % (h.disperser))
      else:
        req.write("<TD>%s</TD>" % (h.filter))

    # Now the 'expamlt' part
    if ('expamlt' in want):
      try:
        req.write("<TD>%.2f</TD>" % h.exptime)
      except:
        req.write("<TD></TD>")
 
      try:
        req.write("<TD>%.2f</TD>" % h.airmass)
      except:
        req.write("<TD></TD>")

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

def progsobserved(session, req, things):
  """
  This function generates a list of programs observed on a given night
  """
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

def tape(session, req, things):
  """
  This is the tape list function
  """
  req.content_type="text/html"
  req.write("<html>")
  req.write("<head><title>FITS Storage tape information</title></head>")
  req.write("<body>")
  req.write("<h1>FITS Storage tape information</h1>")

  # Process form data first
  formdata = util.FieldStorage(req)
  #req.write(str(formdata) )
  for key in formdata.keys():
    field=key.split('-')[0]
    tapeid=int(key.split('-')[1])
    value = formdata[key].value
    if(tapeid):
      tape=session.query(Tape).filter(Tape.id==tapeid).one()
      if(field == 'moveto'):
        tape.location = value
        tape.lastmoved = datetime.datetime.now()
      if(field == 'active'):
        if(value == 'Yes'):
          tape.active = True
        if(value == 'No'):
          tape.active = False
      if(field == 'fate'):
        tape.fate = value
    if(field == 'newlabel'):
      # Add a new tape to the database
      newtape = Tape(value)
      session.add(newtape)

    session.commit()
    
  query = session.query(Tape)
  # Get a list of the tapes that apply
  if(len(things)):
    searchstring = '%'+things[0]+'%'
    query = query.filter(Tape.label.like(searchstring))
  query=query.order_by(Tape.id)
  list = query.all()

  req.write("<HR>")
  for tape in list:
    req.write("<H2>ID: %d, Label: %s</H2>" % (tape.id, tape.label))
    req.write("<UL>")
    req.write("<LI>First Write: %s - Last Write: %s</LI>" % (tape.firstwrite, tape.lastwrite))
    req.write("<LI>Last Verified: %s</LI>" % tape.lastverified)
    req.write("<LI>Location: %s; Last Moved: %s</LI>" % (tape.location, tape.lastmoved))
    req.write("<LI>Active: %s</LI>" % tape.active)
    req.write("<LI>Fate: %s</LI>" % tape.fate)
    req.write("</UL>")

    # The form for modifications
    req.write('<FORM action="/tape" method="post">')
    req.write('<TABLE>')
    # First Row
    req.write('<TR>')
    movekey = "moveto-%d" % tape.id
    req.write('<TD><LABEL for="%s">Move to new location:</LABEL></TD>' % movekey)
    req.write('<TD><INPUT type="text" size=32 name="%s"></TD>' % movekey)
    req.write('</TR>')
    # Second Row
    activekey = "active-%d" % tape.id
    req.write('<TR>')
    req.write('<TD><LABEL for="%s">Active:</LABEL></TD>' % activekey)
    yeschecked = ""
    nochecked = ""
    if(tape.active):
      yeschecked="checked"
    else:
      nochecked="checked"
    req.write('<TD><INPUT type="radio" name="%s" value="Yes" %s>Yes</INPUT> ' % (activekey, yeschecked))
    req.write('<INPUT type="radio" name="%s" value="No" %s>No</INPUT></TD>' % (activekey, nochecked))
    req.write('</TR>')
    # Third Row
    req.write('<TR>')
    fatekey = "fate-%d" % tape.id
    req.write('<TD><LABEL for="%s">Fate:</LABEL></TD>' % fatekey)
    req.write('<TD><INPUT type="text" name="%s" size=32></INPUT></TD>' % fatekey)
    req.write('</TR>')
    req.write('</TABLE>')
    req.write('<INPUT type="submit" value="Save"> <INPUT type="reset">')
    req.write('</FORM>')
    req.write('<HR>')

  req.write('<HR>')
  req.write('<H2>Add a New Tape</H2>')
  req.write('<FORM action="/tape" method="post">')
  req.write('<LABEL for=newlabel-0>Label</LABEL> <INPUT type="text" size=32 name=newlabel-0> <INPUT type="submit" value="Save"> <INPUT type="reset">')
  req.write('</FORM>')

  req.write("</body></html>")
  return apache.OK
