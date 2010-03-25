"""
This is the CadcCRC module. It provides an interface to the CRC function
used by CADC at the GSA.

The full path filename of the executable to run is contained in the
cadcCRC_bin string.
"""
import subprocess
import os
import re

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
