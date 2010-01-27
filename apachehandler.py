# This is the apache python handler
# See /etc/httpd/conf.d/python.conf
# When a request comes in, handler(req) gets called by the apache server

from mod_python import apache
from mod_python import Cookie
from mod_python import util

import sys
import FitsStorage
import FitsStorageUtils
from FitsStorageWebSummary import *

from GeminiMetadataUtils import *

import re
import datetime

import pyfits
# Compile regexps here

orderbycre=re.compile('orderby\=(\S*)')

# The top level handler. This essentially calls out to the specific
# handler function depending on the uri that we're handling
def handler(req):
  # Create the database session here, and close it before we exit
  session = sessionfactory()

  #The next line is for serious debugging
  #return debugmessage(req)

  # Set the no_cache flag on all our output
  # no_cache is not writable, have to set the headers directly
  req.headers_out['Cache-Control'] = 'no-cache'
  req.headers_out['Expired'] = '-1'

  # Parse the uri we were given.
  # This gives everything from the uri below the handler
  # eg if we're handling /python and we're the client requests
  # http://server/python/a/b.fits then we get a/b.fits
  uri = req.uri
  
  # Split this about any /s
  things=uri.split('/')

  # Remove any blanks
  while(things.count('')):
    things.remove('')

  # Check if it's empty
  if(len(things)==0):
    # Empty request
    return usagemessage(req)

  # Before we process the request, parse any arguments into a list
  args=[]
  if(req.args):
    args = req.args.split('&')
    while(args.count('')):
      args.remove('')
 
  # OK, need to parse what we got.

  this = things.pop(0)

  # A debug util
  if(this == 'debug'):
    return debugmessage(req)

  # This is the header summary handler
  if((this == 'summary') or (this == 'diskfiles') or (this == 'ssummary')):
    # Parse the rest of the uri here while we're at it
    # Expect some combination of progid, obsid, date and instrument name
    # We put the ones we got in a dictionary
    selection={}
    while(len(things)):
      thing = things.pop(0)
      if(gemini_date(thing)):
        selection['date']=gemini_date(thing)
      gp=GeminiProject(thing)
      if(gp.progid):
        selection['progid']=thing
      go=GeminiObservation(thing)
      if(go.obsid):
        selection['obsid']=thing
      if(gemini_instrument(thing)):
        selection['inst']=gemini_instrument(thing)
      if(gemini_obstype(thing)):
        selection['obstype']=gemini_obstype(thing)
      if(gemini_obsclass(thing)):
        selection['obsclass']=gemini_obsclass(thing)

    # We should parse the arguments here too
    # All we have for now are order_by arguments
    # We form a list of order_by keywords
    # We should probably do more validation here
    orderby=[]
    for i in range(len(args)):
      match=orderbycre.match(args[i])
      if(match):
        orderby.append(match.group(1))

    return summary(session, req, this, selection, orderby)

  # This returns the full header of the filename that follows.
  if(this ==  'fullheader'):
    if(len(things)):
      filename=things.pop(0)
      filename=fitsfilename(filename)
      return fullheader(session, req, filename)
    else:
      req.content_type="text/plain"
      req.write("You must specify a filename, eg: /fullheader/N20091020S1234.fits\n")
      return apache.OK

  # This returns the fitsverify or wmdreport text from the database
  # you can give it either a diskfile_id or a filename
  if(this == 'fitsverify' or this == 'wmdreport'):
    if(len(things)==0):
      req.content_type="text/plain"
      req.write("You must specify a filename or diskfile_id, eg: /fitsverify/N20091020S1234.fits\n")
      return apache.OK
    thing=things.pop(0)

    # OK, see if we got a filename
    match=re.search('^[NS]20\d\d[01]\d[0123]\dS\d\d\d\d', thing)
    if(match):
      # Ensure it has the .fits on it
      thing = fitsfilename(thing)
      # Now construct the query
      query = session.query(File).filter(File.filename == thing)
      if(query.count()==0):
        req.content_type="text/plain"
        req.write("Cannot find file for: %s\n" % thing)
        return apache.OK
      file = query.one()
      # Query diskfiles to find the diskfile for file that is present
      query = session.query(DiskFile).filter(DiskFile.present == True).filter(DiskFile.file_id == file.id)
      diskfile = query.one()
      req.content_type="text/plain"
      if(this == 'fitsverify'):
        req.write(diskfile.fvreport)
      if(this == 'wmdreport'):
        req.write(diskfile.wmdreport)
      return apache.OK
   
    # See if we got a diskfile_id
    match = re.match('\d+', thing)
    if(match):
      query = session.query(DiskFile).filter(DiskFile.id == thing)
      if(query.count()==0):
        req.content_type="text/plain"
        req.write("Cannot find diskfile for id: %s\n" % thing)
        return apache.OK
      diskfile = query.one()
      req.content_type="text/plain"
      if(this == 'fitsverify'):
        req.write(diskfile.fvreport)
      if(this == 'wmdreport'):
        req.write(diskfile.wmdreport)
      return apache.OK

    # OK, they must have fed us garbage
    req.content_type="text/plain"
    req.write("Could not understand argument - You must specify a filename or diskfile_id, eg: /fitsverify/N20091020S1234.fits\n")
    return apache.OK


  # This is the fits file server
  if(this == 'file'):
    # First, see if we have a valid authorization cookie
    cookies = Cookie.get_cookies(req)
    if(cookies.has_key('gemini_fits_authorization')):
      auth = cookies['gemini_fits_authorization'].value
      if(auth=='good_to_go'):
        # Authenticated OK, find the file in the database
        # Did we get a valid filename?
        if(len(things)==0):
          #req.content_type="text/plain"
          #req.write("You must specify a filename eg: /fits/N20091020S1234.fits\n")
          return apache.HTTP_NOT_FOUND
        filename=things.pop(0)
        match=re.search('^[NS]20\d\d[01]\d[0123]\dS\d\d\d\d', filename)
        if(match):
          # Ensure it has the .fits on it
          filename = fitsfilename(filename)
        else:
          #req.content_type="text/plain"
          #req.write("You must specify a filename eg: /fits/N20091020S1234.fits\n")
          return apache.HTTP_NOT_FOUND
        query=session.query(File).filter(File.filename==filename)
        if(query.count()==0):
          req.content_type="text/plain"
          req.write("Cannot find file for: %s\n" % filename)
          return apache.HTTP_NOT_FOUND
        file=query.one()
        req.sendfile(file.fullpath())
        return apache.OK
      else:
        #req.content_type="text/plain"
        #req.write("Authorization not valid\n")
        return apache.HTTP_FORBIDDEN
    else:
      #req.content_type="text/plain"
      #req.write("Authorization data missing")
      return apache.HTTP_FORBIDDEN

  # This is the projects observed feature
  if(this == "programsobserved"):
    return progsobserved(session, req, things)
    

  # Database Statistics
  if(this == "stats"):
    return stats(session, req)

  # Some static files that the server should serve via a redirect.
  if((this == "robots.txt") or (this == "favicon.ico")):
    newurl = "/htmldocs/%s" % this
    util.redirect(req, newurl)
  
  # Last one on the list - if we haven't return(ed) out of this function
  # by one of the methods above, then we should send out the usage message
  return usagemessage(req)

  session.close()
# End of apache handler() function.
# Below are various helper functions called from above.
# The web summary has it's own module

# This reads the full fits header from the file currently on disk and
# returns in in text form to the browser.
# Arguments are the apache request object and the filename
def fullheader(session, req, filename):
  # If the filename is missing the .fits, then add it
  filename=fitsfilename(filename)

  # First search for a file object with the given filename
  query = session.query(File).filter(File.filename == filename)
  if(query.count()==0):
    req.content_type="text/plain"
    req.write("Cannot find file for: %s\n" % filename)
    return apache.HTTP_NOT_FOUND

  file = query.one()
  hdulist = pyfits.open(file.fullpath(), mode='readonly')
  req.write("FITS File: %s (%s)\n\n" % (filename, file.fullpath()))

  for i in range(len(hdulist)):
    req.write("\n--- HDU %s ---\n" % (i))
    req.write(str(hdulist[i].header.ascardlist()))
    req.write('\n')
  hdulist.close()
  return apache.OK


# Send usage message to browser
def usagemessage(req):
  return util.redirect(req, "/htmldocs/usage.html")

# Send debugging info to browser
def debugmessage(req):
  req.content_type = "text/plain"
  req.write("Debug info\n\n")
  req.write("fits_installation: %s\n\n" % (str(FitsStorage.fits_installation)))
  req.write("python interpreter name: %s\n\n" % (str(req.interpreter)))
  req.write("Pythonpath: %s\n\n" % (str(sys.path)))
  req.write("uri: %s\n\n" % (str(req.uri)))
  req.write("unparsed_uri: %s\n\n" % (str(req.unparsed_uri)))
  req.write("the_request: %s\n\n" % (str(req.the_request)))
  req.write("filename: %s\n\n" % (str(req.filename)))
  req.write("path_info: %s\n\n" % (str(req.path_info)))
  req.write("args: %s\n\n" % (str(req.args)))
  
  return apache.OK

# Send database statistics to browser
def stats(session, req):
  req.content_type = "text/html"
  req.write("<html>")
  req.write("<head><title>FITS Storage database statistics</title></head>")
  req.write("<body>")
  req.write("<h1>FITS Storage database statistics</h1>")

  # File table statistics
  query=session.query(File)
  req.write("<h2>File Table:</h2>")
  req.write("<ul>")
  req.write("<li>Total Rows: %d</li>" % query.count())
  req.write("</ul>")
  
  # DiskFile table statistics
  req.write("<h2>DiskFile Table:</h2>")
  req.write("<ul>")
  # Total rows
  query=session.query(DiskFile)
  totalrows=query.count()
  req.write("<li>Total Rows: %d</li>" % totalrows)
  # Present rows
  query=query.filter(DiskFile.present == True)
  presentrows = query.count()
  percent = 100.0 * presentrows / totalrows
  req.write("<li>Present Rows: %d (%.2f %%)</li>" % (presentrows, percent))
  # Present size
  tpq = session.query(func.sum(DiskFile.size)).filter(DiskFile.present == True)
  tpsize=tpq.one()[0]
  req.write("<li>Total present size: %d bytes (%.02f GB)</li>" % (tpsize, (tpsize/1073741824.0)))
  # most recent entry
  query=session.query(func.max(DiskFile.entrytime))
  latest = query.one()[0]
  req.write("<li>Most recent diskfile entry was at: %s</li>" % latest)
  # Number of entries in last minute / hour / day
  mbefore = datetime.datetime.now() - datetime.timedelta(minutes=1)
  hbefore = datetime.datetime.now() - datetime.timedelta(hours=1)
  dbefore = datetime.datetime.now() - datetime.timedelta(days=1)
  mcount = session.query(DiskFile).filter(DiskFile.entrytime > mbefore).count()
  hcount = session.query(DiskFile).filter(DiskFile.entrytime > hbefore).count()
  dcount = session.query(DiskFile).filter(DiskFile.entrytime > dbefore).count()
  req.write('<LI>Number of DiskFile rows added in the last minute: %d</LI>' % mcount)
  req.write('<LI>Number of DiskFile rows added in the last hour: %d</LI>' % hcount)
  req.write('<LI>Number of DiskFile rows added in the last day: %d</LI>' % dcount)
  # Last 10 entries
  query = session.query(DiskFile).order_by(desc(DiskFile.entrytime)).limit(10)
  list = query.all()
  req.write('<LI>Last 10 diskfile entries added:<UL>')
  for i in list:
    req.write('<LI>%s : %s</LI>' % (i.file.filename, i.entrytime))
  req.write('</UL></LI>')

  
  req.write("</ul>")
  
  # Header table statistics
  query=session.query(Header)
  req.write("<h2>Header Table:</h2>")
  req.write("<ul>")
  req.write("<li>Total Rows: %d</li>" % query.count())
  req.write("</ul>")

  # Data rate statistics
  req.write("<h2>Data Rates</h2>")
  today = datetime.datetime.utcnow().date()
  zerohour = datetime.time(0,0,0)
  ddelta = datetime.timedelta(days=1)
  wdelta = datetime.timedelta(days=7)
  mdelta = datetime.timedelta(days=30)

  start = datetime.datetime.combine(today, zerohour)
  end = start + ddelta
 
  req.write("<h3>Last 10 days</h3><ul>")
  for i in range(10):
    query = session.query(func.sum(DiskFile.size)).select_from(join(Header, DiskFile)).filter(DiskFile.present==True).filter(Header.utdatetime > start).filter(Header.utdatetime < end)
    bytes = query.one()[0]
    if(not bytes):
      bytes = 0
    req.write("<li>%s: %.2f GB</li>" % (str(start.date()), bytes/1E9))
    start -= ddelta
    end -= ddelta
  req.write("</ul>")

  end = datetime.datetime.combine(today, zerohour)
  start = end - wdelta
  req.write("<h3>Last 6 weeks</h3><ul>")
  for i in range(6):
    query = session.query(func.sum(DiskFile.size)).select_from(join(Header, DiskFile)).filter(DiskFile.present==True).filter(Header.utdatetime > start).filter(Header.utdatetime < end)
    bytes = query.one()[0]
    if(not bytes):
      bytes = 0
    req.write("<li>%s - %s: %.2f GB</li>" % (str(start.date()), str(end.date()), bytes/1E9))
    start -= wdelta
    end -= wdelta
  req.write("</ul>")

  end = datetime.datetime.combine(today, zerohour)
  start = end - mdelta
  req.write("<h3>Last 6 pseudo-months</h3><ul>")
  for i in range(6):
    query = session.query(func.sum(DiskFile.size)).select_from(join(Header, DiskFile)).filter(DiskFile.present==True).filter(Header.utdatetime > start).filter(Header.utdatetime < end)
    bytes = query.one()[0]
    if(not bytes):
      bytes = 0
    req.write("<li>%s - %s: %.2f GB</li>" % (str(start.date()), str(end.date()), bytes/1E9))
    start -= mdelta
    end -= mdelta
  req.write("</ul>")

  

  req.write("</body></html>")

  return apache.OK
