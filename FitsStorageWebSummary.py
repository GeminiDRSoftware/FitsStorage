"""
This is the Fits Storage Web Summary module. It provides the functions
which query the database and generate html for the web header
summaries.
"""
from sqlalchemy import or_
from FitsStorage import *
from GeminiMetadataUtils import *
from mod_python import apache, util
import FitsStorageCal

def summary(req, type, selection, orderby):
  """
  This is the main summary generator.
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
  title = "FITS header %s table %s" % (type, sayselection(selection))

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

  session = sessionfactory()
  try:
    webhdrsummary(session, req, type, list_headers(session, selection, orderby))
  except IOError:
    pass
  finally:
    session.close()

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

  query = queryselection(query, selection)

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
  # and limit the number of responses
  if(openquery(selection)):
    query = query.order_by(desc(File.filename))
    query = query.limit(2500)
  else:
    # By default we should order by filename
    query = query.order_by(File.filename)


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

def progsobserved(req, things):
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
  session = sessionfactory()
  try:
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
  except IOError:
    pass
  finally:
    session.close()

def tape(req, things):
  """
  This is the tape list function
  """
  req.content_type="text/html"
  req.write("<html>")
  req.write("<head><title>FITS Storage tape information</title></head>")
  req.write("<body>")
  req.write("<h1>FITS Storage tape information</h1>")

  session = sessionfactory()
  try:
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
  
      # Count Writes
      twq = session.query(TapeWrite).filter(TapeWrite.tape_id == tape.id)
      # Count Bytes
      if(twq.count()):
        bytesquery = session.query(func.sum(TapeWrite.size)).filter(TapeWrite.tape_id == tape.id)
        bytes = bytesquery.one()[0]
      else:
        bytes=0
      req.write('<LI>Writes: <A HREF="/tapewrite/%d">%d</A>, totalling %.2f GB</LI>' % (tape.id, twq.count(), bytes/1.0E9))
        
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
  except IOError:
    pass
  finally:
    session.close()

def tapewrite(req, things):
  """
  This is the tapewrite list function
  """
  req.content_type="text/html"
  req.write("<html>")
  req.write("<head><title>FITS Storage tapewrite information</title></head>")
  req.write("<body>")
  req.write("<h1>FITS Storage tapewrite information</h1>")

  session = sessionfactory()
  try:

    # Find the appropriate TapeWrite entries
    query = session.query(TapeWrite)

    # Can give a tape id (numeric) or label as an argument
    if(len(things)):
      thing = things[0]
      tapeid=0
      try:
        tapeid = int(thing)
      except:
        pass
      if(tapeid):
        query=query.filter(TapeWrite.tape_id == tapeid)
      else:
        thing = '%'+thing+'%'
        tapequery = session.query(Tape).filter(Tape.label.like(thing))
        if(tapequery.count() == 0):
          req.write("<P>Could not find tape by label search</P>")
          req.write("</body></html>")
          session.close()
          return apache.OK
        if(tapequery.count() > 1):
          req.write("<P>Found multiple tapes by label search. Please give the ID instead</P>")
          req.write("</body></html>")
          return apache.OK
        tape = query.one()
        query = query.filter(TapeWrite.tape_id == tape.id)

    query = query.order_by(TapeWrite.startdate)
    tws = query.all()

    for tw in tws:
      req.write("<h2>ID: %d; Tape ID: %d; Tape Label: %s; File Number: %d</h2>" % (tw.id, tw.tape_id, tw.tape.label, tw.filenum))
      req.write("<UL>")
      req.write("<LI>Start Date: %s - End Date: %s</LI>" % (tw.startdate, tw.enddate))
      req.write("<LI>Suceeded: %s</LI>" % tw.suceeded)
      req.write("<LI>Size: %.2f GB</LI>" % (tw.size / 1.0E9))
      req.write("<LI>Status Before: <CODE>%s</CODE></LI>" % tw.beforestatus)
      req.write("<LI>Status After: <CODE>%s</CODE></LI>" % tw.afterstatus)
      req.write("<LI>Hostname: %s, Tape Device: %s</LI>" % (tw.hostname, tw.tapedrive))
      req.write("<LI>Notes: %s</LI>" % tw.notes)
      req.write('<LI>Files: <A HREF="/tapefile/%d">List</A></LI>' % tw.id)
      req.write("</UL>")
  
    req.write("</BODY></HTML>")
    return apache.OK
  except IOError:
    pass
  finally:
    session.close()

def tapefile(req, things):
  """
  This is the tapefile list function
  """
  req.content_type="text/html"
  req.write("<html>")
  req.write("<head>")
  req.write("<title>FITS Storage tapefile information</title>")
  req.write('<link rel="stylesheet" href="/htmldocs/table.css">')
  req.write("</head>")
  req.write("<body>")
  req.write("<h1>FITS Storage tapefile information</h1>")

  if(len(things) != 1):
    req.write("<P>Must supply one argument - tapewrite_id</P>")
    req.write("</body></html>")
    return apache.OK

  tapewrite_id = things[0]

  session = sessionfactory()
  try:
    query=session.query(TapeFile).filter(TapeFile.tapewrite_id == tapewrite_id).order_by(TapeFile.id)

    req.write('<TABLE border=0>')
    req.write('<TR class=tr_head>')
    req.write('<TH>TapeFile ID</TH>')
    req.write('<TH>TapeWrite ID</TH>')
    req.write('<TH>TapeWrite Start Date</TH>')
    req.write('<TH>Tape ID</TH>')
    req.write('<TH>Tape Label</TH>')
    req.write('<TH>File Num on Tape</TH>')
    req.write('<TH>DiskFile ID</TH>')
    req.write('<TH>Filename</TH>')
    req.write('<TH>Size</TH>')
    req.write('<TH>Last Modified</TH>')
    req.write('</TR>')
  
    even=0
    for tf in query.all():
      even = not even
      if(even):
        cs = "tr_even"
      else:
        cs = "tr_odd"
      # Now the Table Row
      req.write("<TR class=%s>" % (cs))
      req.write("<TD>%d</TD>" % tf.id)
      req.write("<TD>%d</TD>" % tf.tapewrite_id)
      req.write("<TD>%s</TD>" % tf.tapewrite.startdate)
      req.write("<TD>%d</TD>" % tf.tapewrite.tape.id)
      req.write("<TD>%s</TD>" % tf.tapewrite.tape.label)
      req.write("<TD>%d</TD>" % tf.tapewrite.filenum)
      req.write("<TD>%d</TD>" % tf.diskfile_id)
      req.write("<TD>%s</TD>" % tf.diskfile.file.filename)
      req.write("<TD>%s</TD>" % tf.diskfile.size)
      req.write("<TD>%s</TD>" % tf.diskfile.lastmod)
      req.write("</TR>")

    req.write("</TABLE></BODY></HTML>")
    return apache.OK
  except IOError:
    pass
  finally:
    session.close()

def calibrations(req, type, selection):
  """
  This is the calibrations generator.
  req is an apache request handler request object
  type is the summary type required
  selection is an array of items to select on, simply passed
    through to the webhdrsummary function

  returns an apache request status code
  """
  req.content_type = "text/html"
  req.write("<html>")
  title = "Calibrations %s" % sayselection(selection)

  req.write("<head>")
  req.write("<title>%s</title>" % (title))
  req.write('<link rel="stylesheet" href="/htmldocs/table.css">')
  req.write("</head>\n")
  req.write("<body>")
  if (fits_system_status == "development"):
    req.write('<h1>This is the development system, please use <a href="http://fits/">fits</a> for operational use</h1>')
  req.write("<H1>%s</H1>" % (title))

  session = sessionfactory()
  try:
    # OK, find the target files
    # The Basic Query
    query = session.query(Header).select_from(join(Header, join(DiskFile, File)))

    # Only the canonical versions
    selection['canonical'] = True

    query = queryselection(query, selection)

    # Knock out the FAILs
    query = query.filter(Header.rawgemqa!='BAD')


    # If openquery, limit number of responses
    if(openquery(selection)):
      query = query.limit(2500)

    # OK, do the query
    headers = query.all()

    req.write("<H2>Found %d datasets to check for suitable calibrations</H2>" % len(headers))
    req.write("<HR>")

    # Was the request for only one type of calibration?
    caltype='all'
    if('caltype' in selection):
      caltype = selection['caltype']

    warnings = 0
    missings = 0
    for object in headers:
      # Accumulate the html in a string, so we can decide whether to display it all at once
      html=""
      takenow = False
      warning=False
      missing=False
      requires=False

      html+='<H3><a href="/fullheader/%s">%s</a> - <a href="/summary/%s">%s</a></H3>' % (object.diskfile.file.filename, object.diskfile.file.filename, object.datalab, object.datalab)

      c = FitsStorageCal.get_cal_object(session, None, header=object)
      if('arc' in c.required and (caltype=='all' or caltype=='arc')):
        requires=True

        # Look for an arc in the same program
        arc = c.arc(sameprog=True)

        if(arc):
          html += '<H4>ARC: <a href="/fullheader/%s">%s</a> - <a href="/summary/%s">%s</a></H4>' % (arc.diskfile.file.filename, arc.diskfile.file.filename, arc.datalab, arc.datalab)
          if(arc.utdatetime and object.utdatetime):
            html += "<P>arc was taken %s object</P>" % interval_string(arc, object)
            if(abs(interval_hours(arc, object)) > 120):
              html += '<P><FONT COLOR="Red">WARNING - this is more than 5 days different</FONT></P>'
              warning = True
              arc_a=arc.id
          else:
            html += '<P><FONT COLOR="Red">Hmmm, could not determine time delta...</FONT></P>'
            warning = True

        else:
          html += '<H3><FONT COLOR="Red">NO ARC FOUND!</FONT></H3>'
          warning = True
          missing = True

        # If we didn't find one in the same program or we did, but with warnings,
        # Re-do the search accross all program IDs
        if(warning):
          oldarc = arc
          arc = c.arc()
          # If we find a different one
          if(arc and oldarc and (arc.id != oldarc.id)):
            missing = False
            html += '<H4>ARC: <a href="/fullheader/%s">%s</a> - <a href="/summary/%s">%s</a></H4>' % (arc.diskfile.file.filename, arc.diskfile.file.filename, arc.datalab, arc.datalab)
            if(arc.utdatetime and object.utdatetime):
              html += "<P>arc was taken %s object</P>" % interval_string(arc, object)
              if(abs(interval_hours(arc, object)) > 120):
                html += '<P><FONT COLOR="Red">WARNING - this is more than 5 days different</FONT></P>'
                warning = True
            else:
              html += '<P><FONT COLOR="Red">Hmmm, could not determine time delta...</FONT></P>'
              warning = True
            if(arc.progid != object.progid):
              html += '<P><FONT COLOR="Red">WARNING: ARC and OBJECT come from different project IDs.</FONT></P>'
              warning = True

      if('bias' in c.required and (caltype=='all' or caltype=='bias')):
        requires=True
        bias = c.bias()
        if(bias):
          html += "<H4>BIAS: %s - %s</H4>" % (bias.diskfile.file.filename, bias.datalab)
        else:
          html += '<H3><FONT COLOR="Red">NO BIAS FOUND!</FONT></H3>'
          warning = True
          missing = True

      html += "<HR>"

      # Handle the 'takenow' flag. This should get set to true if
      # no arc exists or 
      # all the arcs generate warnings, and
      # the time difference between 'now' and the science frame is 
      # less than the time difference between the science frame and the closest
      # arc to it that we currently have
      if(missing):
        takenow = True
      if(warning):
        # Is it worth re-taking?
        # Find the smallest interval between a valid arc and the science
        oldinterval = None
        newinterval = None
        smallestinterval = None
        if (oldarc):
          oldinterval = abs(interval_hours(oldarc, object))
          smallestinterval = oldinterval
        if (arc):
          newinterval = abs(interval_hours(arc, object))
          smallestinterval = newinterval
        if (oldinterval and newinterval):
          if (oldinterval > newinterval):
            smallestinterval = newinterval
          else:
            smallestinterval = oldinterval
        # Is the smallest interval larger than the interval between now and the science?
        now = datetime.datetime.utcnow()
        then = object.utdatetime
        nowinterval = now - then
        nowhours = (nowinterval.days * 24.0) + (nowinterval.seconds / 3600.0)
        if(smallestinterval > nowhours):
          takenow=True

   
      caloption=None
      if('caloption' in selection):
        caloption = selection['caloption']

      if(caloption=='warnings'):
        if(warning):
          req.write(html)
      elif(caloption=='missing'):
        if(missing):
          req.write(html)
      elif(caloption=='requires'):
        if(requires):
          req.write(html)
      elif(caloption=='takenow'):
        if(takenow):
          req.write(html)
      else:
        req.write(html)
      if(warning):
        warnings +=1
      if(missing):
        missings +=1

    req.write("<HR>")
    req.write("<H2>Counted %d potential missing Calibrations</H2>" % missings)
    req.write("<H2>Query generated %d warnings</H2>" % warnings)
    req.write("</body></html>")
    return apache.OK

  except IOError:
    pass
  finally:
    session.close()

def interval_hours(a, b):
  """
  Given two header objects, returns the number of hours b was taken after a
  """
  interval = a.utdatetime - b.utdatetime
  tdelta = (interval.days * 24.0) + (interval.seconds / 3600.0)

  return tdelta

def interval_string(a, b):
  """
  Given two header objects, return a human readable string describing the time difference between them
  """
  t = interval_hours(a, b)

  word = "after"
  unit = "hours"

  if(t < 0.0):
    word = "before"
    t *= -1.0

  if(t > 48.0):
    t /= 24.0
    unit = "days"

  if(t < 1.0):
    t *= 60
    unit = "minutes"

  # 1.2 days after
  string = "%.1f %s %s" % (t, unit, word)

  return string

def sayselection(selection):
  """
  returns a string that describes  the selection dictionary passed in
  suitable for pasting into html
  """
  string = ""

  if('progid' in selection):
    string += "; Program ID: %s" % selection['progid']
  if('obsid' in selection):
    string += "; Observation ID: %s" % selection['obsid']
  if('datalab' in selection):
    string += "; Data Label: %s" % selection['datalab']
  if('date' in selection):
    string += "; Date: %s" % selection['date']
  if('daterange' in selection):
    string += "; Daterange: %s" % selection['daterange']
  if('inst' in selection):
    string += "; Instrument: %s" % selection['inst']
  if('obstype' in selection):
    string += "; ObsType: %s" % selection['obstype']
  if('obsclass' in selection):
    title += "; ObsClass: %s" % selection['obsclass']
  if('filename' in selection):
    string += "; Filename: %s" % selection['filename']
  if('gmos_grating' in selection):
    string += "; GMOS Grating: %s" % selection['gmos_grating']
  if('gmos_fpmask' in selection):
    string += "; GMOS FP Mask: %s" % selection['gmos_fpmask']
  if('caltype' in selection):
    string += "; Calibration Type: %s" % selection['caltype']
  if('caloption' in selection):
    string += "; Calibration Option: %s" % selection['caloption']

  return string

# import time module to get local timezone
import time
def queryselection(query, selection):
  """
  Given an sqlalchemy query object and a selection dictionary,
  add filters to the query for the items in the selection
  and return the query object
  """

  # Do want to select Header object for which diskfile.present is true?
  if('present' in selection):
    query = query.filter(DiskFile.present == selection['present'])

  if('canonical' in selection):
    query = query.filter(DiskFile.present == selection['canonical'])

  if('progid' in selection):
    query = query.filter(Header.progid==selection['progid'])

  if('obsid' in selection):
    query = query.filter(Header.obsid==selection['obsid'])

  if('datalab' in selection):
    query = query.filter(Header.datalab==selection['datalab'])

  # Should we query by date?
  if('date' in selection):
    # Parse the date to start and end datetime objects
    # We consider the night boundary to be 14:00 local time
    # This is midnight UTC in Hawaii, completely arbitrary in Chile
    startdt = dateutil.parser.parse("%s 14:00:00" % (selection['date']))
    tzoffset = datetime.timedelta(seconds=time.timezone)
    oneday = datetime.timedelta(days=1)
    startdt = startdt + tzoffset - oneday
    enddt = startdt + oneday
    # check it's between these two
    query = query.filter(Header.utdatetime >= startdt).filter(Header.utdatetime <= enddt)

  # Should we query by daterange?
  if('daterange' in selection):
    # Parse the date to start and end datetime objects
    daterangecre=re.compile('(20\d\d[01]\d[0123]\d)-(20\d\d[01]\d[0123]\d)')
    m = daterangecre.match(selection['daterange'])
    startdate = m.group(1)
    enddate = m.group(2)
    tzoffset = datetime.timedelta(seconds=time.timezone)
    oneday = datetime.timedelta(days=1)
    startdt = dateutil.parser.parse("%s 14:00:00" % startdate)
    startdt = startdt + tzoffset - oneday
    enddt = dateutil.parser.parse("%s 14:00:00" % enddate)
    enddt = enddt + tzoffset - oneday
    enddt = enddt + oneday
    # Flip them round if reversed
    if(startdt > enddt):
      tmp = enddt
      enddt = startdt
      started = tmp
    # check it's between these two
    query = query.filter(Header.utdatetime >= startdt).filter(Header.utdatetime <= enddt)

  if('obstype' in selection):
    query = query.filter(Header.obstype==selection['obstype'])

  if('obsclass' in selection):
    query = query.filter(Header.obsclass==selection['obsclass'])

  if('inst' in selection):
    query = query.filter(Header.instrument==selection['inst'])

  if('filename' in selection):
    query = query.filter(File.filename == selection['filename'])

  if('gmos_grating' in selection):
    query = query.filter(Header.disperser == selection['gmos_grating'])

  if('gmos_fpmask' in selection):
    query = query.filter(Header.fpmask == selection['gmos_fpmask'])

  return query

def openquery(selection):
  """
  Returns a boolean to say if the selection is limited to a reasonable number of
  results - ie does it contain a date, daterange, prog_id, obs_id etc
  returns True if this selection will likely return a large number of results
  """
  openquery = True

  things = ['date', 'daterange', 'progid', 'obsid', 'datalab', 'filename']

  for thing in things:
    if(thing in selection):
      openquery = False

  return openquery
