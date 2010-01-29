import os
import sys
import subprocess
from FitsStorageConfig import *

# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--read", action="store_true", dest="read", help="Read the label from the tape in the drive")
parser.add_option("--label", action="store", dest="label", help="Write the label to the tape in the drive. This will write to the start of the tape, making any other data on the tape inaccessible"
parser.add_option("--force", action="store_true", dest="force", help="Normally, --label will refuse to label a tape that allready contains a tapelabel. This option forces it to do so.")

(options, args) = parser.parse_args()

if((not options.read) and (not options.label)):
  print "You must supply either the --read or the --label option"
  sys.exit(1)

# cd to tapescratch directory
try:
  os.chdir(fits_tape_scratchdir)
except:
  print "Could not chdir to scratch directory: %s" % fits_tape_scratchdir
  sys.exit(1)

# make a subdir for this pid
pid = str(os.getpid())
try:
  os.mkdir(pid)
  os.chdir(pid)
except:
  print "could not mkdir and chdir to %s" % pid
  sys.exit(1)

# rewind the tape
retval = subprocess.call(['/bin/mt', '-f', fits_tape_device, 'rewind'])
if(retval):
  print "Rewind failed with exit code: %d" % retval
  sys.exit(retval)

# setblk 0 the tape device
retval = subprocess.call(['/bin/mt', '-f', fits_tape_device, 'setblk', '0'])
if(retval):
  print "setblk failed with exit code: %d" % retval
  sys.exit(retval)

# try to use tar to extract the tapelabel file from the first file on the tape
readok=0
label=''
retval = subprocess.call('/bin/tar', 'xf', fits_tape_device, 'tapelabel')
if(retval):
  # OK, failed to read the tapelabel file from the tape
  if(options.read):
    print "tar x tapelabel failed with exit code: %d" % retval
    sys.exit(retval)
else:
  # OK, managed to read a file called tapelabel
  # Now try and rewind again
  retval = subprocess.call(['/bin/mt', '-f', fits_tape_device, 'rewind'])
    if(retval):
      print "Rewind failed with exit code: %d" % retval
      sys.exit(retval)

  # read in the first line of the tapelabel file and delete the file
  try:
    f = open('tapelabel', 'r')
    label = f.readline()
    f.close()
    os.unlink('tapelabel')
    label = label.strip()
    readok=1
    if(options.read):
      print label
      sys.exit(0)
  except:
    print "could not read label from tapelabel file"
    sys.exit(1)

if(options.label):
  if(readok and (not options.force)):
    print "Tape is already labeled: %s" % label
    print "Will not overwrite tape. Use --force to force"
    sys.exit(1)

if(options.label and ((not readok) or options.force)):
  # Label the tape, really.
  try:
    f = open('tapelabel', 'w')
    f.write(options.label)
    f.close()
  except:
    print "could not create tapelabel file"
    sys.exit(1)

  # tar the tapelabel file to the tape
  retval = subprocess.call('/bin/tar', 'cf', fits_tape_device, 'tapelabel')
  if(retval):
    # OK, failed to write the tapelabel file to the tape
    print "tar c tapelabel failed with exit code: %d" % retval
    sys.exit(retval)

# Try to clean up
try:
  os.unlink('tapelabel')
  os.chdir(fits_tape_scratchdir)
  os.rmdir(pid)

except:
  pass

sys.exit(0)
