import subprocess
import os
import re

# the path to the fitsverify binary
wmd = ['/astro/i686/jre1.5.0_03/bin/java', '-Djava.library.path=/data/extern/mdIngest/lib.x86_fedora/', '-D"ca.nrc.cadc.configDir=/data/extern/mdIngest/config"', '-jar', '/data/extern/mdIngest/lib/mdIngest.jar', '--archive=GEMINI', '-c', '-d']

# Compile the regular expression here for efficiency
cre=re.compile('File \S* (IS|IS NOT) ready for ingestion')

def cadcWMD(filename):
  # First check that the filename exists is readable and is a file
  exists = os.access(filename, os.F_OK | os.R_OK)
  isfile = os.path.isfile(filename)
  if(not(exists and isfile)):
    print "%s is not readable or is not a file" % (filename)
    return

  wmd_arg = "--file=%s" % filename
  wmd.append(wmd_arg)
  env=os.environ
  env['LD_LIBRARY_PATH']='/data/extern/mdIngest/lib.x86_fedora'
  # Fire off the subprocess and capture the output
  sp = subprocess.Popen(wmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  (stdoutstring, stderrstring) = sp.communicate()

  match=cre.search(stdoutstring)
  if(match):
    isit = match.group(1)
    itis=0
    if(isit=="IS"):
      itis=1
    if(ISIT=="IS NOT"):
      itis=0
  else:
    print "Could not match cadcCRC return value"
    itis = 0

  return (itis, stdoutstring)
