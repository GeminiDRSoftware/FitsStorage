# This is the apache python handler
# See /etc/httpd/conf.d/python.conf
# When a request comes in, handler(req) gets called by the apache server

from mod_python import apache

import sys
import FitsStorage
from FitsStorageWebSummary import *

import re


# The top level handler. This essentially calls out to the specific
# handler function depending on the uri that we're handling

def handler(req):
  #The next line is for serious debugging
  #return debugmessage(req)

  # This gives everything from the uri below the handler
  # eg if we're handling /python and we're the client requests
  # http://server/python/a/b.fits then we get a/b.fits
  request = req.uri
  
  # Split this about and /s
  things=request.split('/')

  # Remove any blanks
  while(things.count('')):
    things.remove('')

  # Check if it's empty
  if(len(things)==0):
    # Empty request
    return usagemessage(req)

  # OK, need to parse what we got.

  this = things.pop(0)

  # A debug util
  if(this == 'debug'):
    return debugmessage(req)

  # This is the header summary handler
  if(this == 'summary'):
    # Parse the rest here while we're at it
    # Expect some combination of progid, obsid and date
    date=''
    progid=''
    obsid=''
    while(len(things)):
      thing = things.pop(0)
      if(re.match("20\d\d[01]\d[0123]\d", thing)):
        date=thing
      if(re.match("G[NS]-20\d\d[AB]-[A-Z]*-\d*$", thing)):
        progid=thing
      if(re.match("G[NS]-20\d\d[AB]-[A-Z]*-\d*-\d*$", thing)):
        obsid=thing
    return summary(req, progid, obsid, date)

  # This returns the full header of the filename that follows.
  if(this ==  'fullheader'):
    if(len(things)):
      filename=things.pop(0)
      return fullheader(req, filename)
    else:
      req.content_type="text/plain"
      req.write("You must specify a filename, eg: /fullheader/N20091020S1234.fits\n")
      return apache.OK

  # This returns the fitsverify text from the database
  # you can give it either a diskfile_id or a filename
  if(this == 'fitsverify'):
    if(len(things)==0):
      req.content_type="text/plain"
      req.write("You must specify a filename or diskfile_id, eg: /fitsverify/N20091020S1234.fits\n")
      return apache.OK
    thing=things.pop(0)

    # OK, see if we got a filename
    match=re.search('^[NS]20\d\d[01]\d[0123]\dS\d\d\d\d', thing)
    if(match):
      # Ensure it has the .fits on it
      match = re.match('\S*.fits$', thing)
      if(not match):
        thing = "%s.fits" % (thing)
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
      req.write(diskfile.fvreport)
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
      req.write(diskfile.fvreport)
      return apache.OK

    # OK, they must have fed us garbage
    req.content_type="text/plain"
    req.write("Could not understand argument - You must specify a filename or diskfile_id, eg: /fitsverify/N20091020S1234.fits\n")
    return apache.OK

  # Last one on the list - if we haven't return(ed) out of this function
  # by one of the methods above, then we should send out the usage message
  return usagemessage(req)

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
  req.write('<LI>In fact you can use any combination of date, obsid and progid in the URL and it will combine them with a logical and</LI>')
  req.write('</UL></LI>')
  req.write('<LI><a href="/fullheader/N20091123S0455.fits">/fullheader/N20091123S0455.fits</a> or <a href="/fullheader/N20091123S0455">/fullheader/N20091123S0455</a> gives the full fits header of that file</LI>')
  req.write('<LI><a href="/fitsverify/N20091120S0003.fits">/fitsverify/N20091120S0003.fits</a> or <a href="/fitsverify/N20091120S0003">/fitsverify/N20091120S0003</a> gives the fitsverify report for that file. The filename maybe replaced by a database diskfile_id for internal links to specific diskfile instances, but these should not be used externally as the id may change</LI>')
  req.write('</UL>')
  req.write('</body></html>')
  return apache.OK


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

