# This is the apache python handler
# See /etc/httpd/conf.d/python.conf
# When a request comes in, handler(req) gets called by the apache server

from mod_python import apache
from mod_python import Cookie

import sys
import FitsStorage
from FitsStorageUtils import *
from FitsStorageWebSummary import *

import re

# Compile regexps here
datecre=re.compile('20\d\d[01]\d[0123]\d')
progidcre=re.compile('G[NS]-20\d\d[AB]-[A-Z]*-\d*$')
obsidcre=re.compile('G[NS]-20\d\d[AB]-[A-Z]*-\d*-\d*$')
niricre=re.compile('[Nn][Ii][Rr][Ii]')
nifscre=re.compile('[Nn][Ii][Ff][Ss]')
gmosncre=re.compile('[Gg][Mm][Oo][Ss]-[Nn]')
michellecre=re.compile('[Mm][Ii][Cc][Hh][Ee][Ll][Ll][Ee]')

orderbycre=re.compile('orderby\=(\S*)')

# The top level handler. This essentially calls out to the specific
# handler function depending on the uri that we're handling
def handler(req):
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
  if((this == 'summary') or (this == 'diskfiles')):
    # Parse the rest of the uri here while we're at it
    # Expect some combination of progid, obsid, date and instrument name
    # We put the ones we got in a dictionary
    selection={}
    while(len(things)):
      thing = things.pop(0)
      if(datecre.match(thing)):
        selection['date']=thing
      if(progidcre.match(thing)):
        selection['progid']=thing
      if(obsidcre.match(thing)):
        selection['obsid']=thing
      if(niricre.match(thing)):
        selection['inst']='NIRI'
      if(nifscre.match(thing)):
        selection['inst']='NIFS'
      if(gmosncre.match(thing)):
        selection['inst']='GMOS-N'
      if(michellecre.match(thing)):
        selection['inst']='MICHELLE'

    # We should parse the arguments here too
    # All we have for now are order_by arguments
    # We form a list of order_by keywords
    # We should probably do more validation here
    orderby=[]
    for i in range(len(args)):
      match=orderbycre.match(args[i])
      if(match):
        orderby.append(match.group(1))

    return summary(req, this, selection, orderby)

  # This returns the full header of the filename that follows.
  if(this ==  'fullheader'):
    if(len(things)):
      filename=things.pop(0)
      filename=fitsfilename(filename)
      return fullheader(req, filename)
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
  if(this == 'fits'):
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

  # Database Statistics
  if(this == "stats"):
    return stats(req)

  # Last one on the list - if we haven't return(ed) out of this function
  # by one of the methods above, then we should send out the usage message
  return usagemessage(req)

# End of apache handler() function.
# Below are various helper functions called from above.
# The web summary has it's own module

# This reads the full fits header from the file currently on disk and
# returns in in text form to the browser.
# Arguments are the apache request object and the filename
def fullheader(req, filename):
  # If the filename is missing the .fits, then add it
  filename=fitsfilename(filename)

  # First search for a file object with the given filename
  query = session.query(File).filter(File.filename == filename)
  if(query.count()==0):
    req.content_type="text/plain"
    req.write("Cannot find file for: %s\n" % filename)
    return apache.HTTP_NOT_FOUND

  file = query.one()
  hdulist = pyfits.open(file.fullpath())
  req.write("FITS File: %s (%s)\n\n" % (filename, file.fullpath()))

  for i in range(len(hdulist)):
    req.write("\n--- HDU %s ---\n" % (i))
    req.write(str(hdulist[i].header.ascardlist()))
    req.write('\n')
  hdulist.close()
  return apache.OK


# Send usage message to browser
def usagemessage(req):
  req.content_type = "text/html"
  req.write('<html')
  req.write('<head><title>FITS storage web help page</title></head>')
  req.write('<body>')
  req.write('<H1>FITS storage web help page</H1>')
  req.write('<UL>')
  req.write('<LI><a href="/">/</a> or <a href="/help">/help</a> gives this help page</LI>')
  req.write('<LI>/summary gives file/header summaries:<UL>')
  req.write('<LI><a href="/summary">/summary</a> by itself shows the last 2500 files, newest first</LI>')
  req.write('<LI><a href="/summary/20091120">/summary/20091120</a> shows the data from 20091120, in order</LI>')
  req.write('<LI><a href="/summary/GN-2009B-Q-51">/summary/GN-2009B-Q-51</a> shows all data for program GN-2009B-Q-51</LI>')
  req.write('<LI><a href="/summary/GN-2009B-Q-51-15">/summary/GN-2009B-Q-51-15</a> shows all data for observatio GN-2009B-Q-51-15</LI>')
  req.write('<LI><a href="/summary/GN-2009B-Q-51/20091123">/summary/GN-2009B-Q-51/20091123</a> or indeed <a href="/summary/20091123/GN-2009B-Q-51">/summary/20091123/GN-2009B-Q-51</a> shows all the data for GN-2009B-Q-51 taken on 20091123</LI>')
  req.write('<LI><a href="/summary/20091120/NIRI">/summary/20091120/NIRI</a> shows all the files for that date using that instrument</LI>')
  req.write('<LI>In fact you can use any combination of date, obsid, progid and instrument in the URL and it will combine them with a logical and</LI>')
  req.write('<LI>You can add ?orderby=somethign arguments to the summary to change the sorting - see the urls linked via the arrows in the table headers for examples</LI>')
  req.write('</UL></LI>')
  req.write('<LI><a href="/fullheader/N20091123S0455.fits">/fullheader/N20091123S0455.fits</a> or <a href="/fullheader/N20091123S0455">/fullheader/N20091123S0455</a> gives the full fits header of that file</LI>')
  req.write('<LI><a href="/fitsverify/N20091120S0003.fits">/fitsverify/N20091120S0003.fits</a> or <a href="/fitsverify/N20091120S0003">/fitsverify/N20091120S0003</a> gives the fitsverify report for that file. The filename maybe replaced by a database diskfile_id for internal links to specific diskfile instances, but these should not be used externally as the id may change</LI>')
  req.write('</UL>')
  req.write('</body></html>')
  return apache.OK


# Send debugging info to browser
def debugmessage(req):
  req.content_type = "text/plain"
  req.write("Debug info\n\n")
  req.write("Pythonpath: %s\n\n" % (str(sys.path)))
  req.write("uri: %s\n\n" % (str(req.uri)))
  req.write("unparsed_uri: %s\n\n" % (str(req.unparsed_uri)))
  req.write("the_request: %s\n\n" % (str(req.the_request)))
  req.write("filename: %s\n\n" % (str(req.filename)))
  req.write("path_info: %s\n\n" % (str(req.path_info)))
  req.write("args: %s\n\n" % (str(req.args)))
  
  return apache.OK

# Send database statistics to browser
def stats(req):
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
  query=session.query(DiskFile)
  req.write("<h2>DiskFile Table:</h2>")
  req.write("<ul>")
  req.write("<li>Total Rows: %d</li>" % query.count())
  query=query.filter(DiskFile.present == True)
  presentrows = query.count()
  req.write("<li>Present Rows: %d</li>" % presentrows)
  tpq = session.query(func.sum(DiskFile.size)).filter(DiskFile.present == True)
  tpsize=tpq.one()[0]
  req.write("<li>Total present size: %d bytes (%.02f GB)</li>" % (tpsize, (tpsize/1073741824.0)))
  req.write("</ul>")
  
  # Header table statistics
  query=session.query(Header)
  req.write("<h2>Header Table:</h2>")
  req.write("<ul>")
  req.write("<li>Total Rows: %d</li>" % query.count())
  req.write("</ul>")
  

  req.write("</body></html>")

  return apache.OK
