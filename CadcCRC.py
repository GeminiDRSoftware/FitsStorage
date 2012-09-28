"""
This is the CadcCRC module. It provides an interface to the CRC function
used by CADC at the GSA.

The full path filename of the executable to run is contained in the
cadcCRC_bin string.

This module also contains an md5sum function for convenience.
"""
import subprocess
import os
import re
import urllib2
import dateutil.parser

# the path to the fitsverify binary
cadcCRC_bin = '/opt/cadc/cadcCRC'

# Compile the regular expression here for efficiency
cre=re.compile('\S*\s*([0123456789abcdef]*)\n')

def cadcCRC(filename):
  """
  Runs the executable on the specified filename.
  Retuns a string containing the CRC string.
  """
  # First check that the filename exists is readable and is a file
  exists = os.access(filename, os.F_OK | os.R_OK)
  isfile = os.path.isfile(filename)
  if(not(exists and isfile)):
    print "%s is not readable or is not a file" % (filename)
    return

  # Fire off the subprocess and capture the output
  sp = subprocess.Popen([cadcCRC_bin, filename], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  (stdoutstring, stderrstring) = sp.communicate()

  match=cre.match(stdoutstring)
  if(match):
    retary = match.group(1)
  else:
    print "Could not match cadcCRC return value"
    retary=None

  return retary
  
def md5sumfile(f):
  """
  Generates the md5sum of the thing represented by the file object f
  """
  import hashlib
  m = hashlib.md5()

  block = 64*1024
  data = f.read(block)
  m.update(data)
  while(data):
    data = f.read(block)
    m.update(data)

  return m.hexdigest()

def md5sum(filename):
  """
  Generates the md5sum of the filename, returns the hex string.
  """
  f = open(filename, 'r')
  m = md5sumfile(f)
  f.close()
  return m


def get_gsa_info(filename, user, passwd):
  """
  Queries the GSA for the given filename, using authentication details provided.
  returns a dict containing md5sum and ingestdate keys
  """

  # Create an authenticated urllib2 request object set to do http HEAD
  class HeadRequest(urllib2.Request):
    def get_method(self):
      return "HEAD"
  auth_handler = urllib2.HTTPBasicAuthHandler()
  auth_handler.add_password(realm='Canadian Astronomy Data Centre', uri='http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/auth/GEMINI', user=user, passwd=passwd)
  opener = urllib2.build_opener(auth_handler)
  urllib2.install_opener(opener)

  # Form the URL and get the http header. Doesn't fetch the actual data
  url = 'http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/auth/GEMINI/%s' % filename

  request = HeadRequest(url)
  try:
    response = urllib2.urlopen(request)
    headers = response.info()
    response.close()
    http_error=0
  except urllib2.HTTPError:
    http_error=1

  # Make the empty return dictionary
  dict = {}

  if http_error:
    dict['md5sum']=None
    dict['ingestdate']=None
  else:
    # Put the MD5sum from the header into the dict
    dict['md5sum']=response.headers.get('X-Uncompressed-MD5')

    # Get the ingest date string and parse it into the dict
    ids = response.headers.get('Last-Modified')
    id = dateutil.parser.parse(ids)
    dict['ingestdate'] = id

  return dict
