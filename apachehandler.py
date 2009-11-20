# This is the apache python handler
# See /etc/httpd/conf.d/python.conf
# When a request comes in, handler(req) gets called by the apache server

from mod_python import apache

import sys
import FitsStorage
from FitsStorageWebSummary import webhdrsummary, list_headers

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

  # Last one on the list - if we haven't return(ed) out of this function
  # by one of the methods above, then we should send out the usage message
  return usagemessage(req)

def usagemessage(req):
  req.content_type = "text/plain"
  req.write("This is the fits storage web help\n")
  req.write("/ or /help returns this text\n")
  req.write("/summary gives file / header summaries")
  return apache.OK


def debugmessage(req):
  req.content_type = "text/plain"
  req.write("Debug info\n\n")
  req.write("Pythonpath: %s\n" % (str(sys.path)))
  req.write("uri: %s\n" % (str(req.uri)))
  return apache.OK

def summary(req, progid, obsid, date):
  req.content_type = "text/plain"
  req.write("This is the python summary handler\n")
  req.write("%s: %s\n" %('progid', progid))
  req.write("%s: %s\n" %('obsid', obsid))
  req.write("%s: %s\n" %('date', date))
  webhdrsummary(req, list_headers(progid, obsid, date))
  return apache.OK


