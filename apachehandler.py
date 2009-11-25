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

  if(this ==  'fullheader'):
    # This returns the full header of the filename that follows.
    if(len(things)):
      filename=things.pop(0)
      return fullheader(req, filename)
    else:
      req.content_type="text/plain"
      req.write("You must specify a filename, eg: /fullheader/N20091020S1234.fitsi\n")
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
  req.write('</UL></LI></UL>')
  req.write('</body></html>')
  return apache.OK


def debugmessage(req):
  req.content_type = "text/plain"
  req.write("Debug info\n\n")
  req.write("Pythonpath: %s\n" % (str(sys.path)))
  req.write("uri: %s\n" % (str(req.uri)))
  return apache.OK

