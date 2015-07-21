import urllib
from xml.dom.minidom import parseString
import os
import re
import hashlib
import sys

# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--tapeserver", action="store", type="string", dest="tapeserver", default="hbffitstape1", help="The Fits Storage Tape server to use to check the files are on tape")
parser.add_option("--file-pre", action="store", type="string", dest="filepre", help="File prefix to operate on, eg N20090130, N200812 etc")
parser.add_option("--mintapes", action="store", type="int", dest="mintapes", default=2, help="Minimum number of tapes file must be on to be eligable for deletion")
parser.add_option("--nomd5", action="store_true", dest="nomd5", help="Do not check md5, match on filename only")
parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Dry Run - do not actually do anything")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

rawlist = os.listdir('.')
thelist = []
restring = '^' + options.filepre + '.*'
cre = re.compile(restring)
for i in rawlist:
  if(cre.match(i)):
    thelist.append(i)

print "Files to consider: %s" % thelist

for thefile in thelist:

  m = hashlib.md5()
  block = 64*1024
  f = open(thefile, 'r')
  data = f.read(block)
  m.update(data)
  while(data):
    data = f.read(block)
    m.update(data)
  f.close()
  filemd5 = m.hexdigest()

  print "Considering %s - %s" % (thefile, filemd5)

  url = "http://%s/fileontape/%s" % (options.tapeserver, thefile)

  u = urllib.urlopen(url)
  xml = u.read()
  u.close()

  dom = parseString(xml)

  fileelements = dom.getElementsByTagName("file")

  tapeids = []
  for fe in fileelements:
    filename = fe.getElementsByTagName("filename")[0].childNodes[0].data
    md5 = fe.getElementsByTagName("md5")[0].childNodes[0].data
    tapeid = int(fe.getElementsByTagName("tapeid")[0].childNodes[0].data)
    if((filename == thefile) and ((md5 == filemd5) or options.nomd5) and (tapeid not in tapeids)):
      #print "Found it on tape id %d" % tapeid
      tapeids.append(tapeid)

  if(len(tapeids) >= options.mintapes):
    if(options.dryrun):
      print "Dry run - not actually deleting File %s - %s which is on %d tapes: %s" % (thefile, filemd5, len(tapeids), tapeids)
    else:
      print "Deleting File %s - %s which is on %d tapes: %s" % (thefile, filemd5, len(tapeids), tapeids)
      try:
        os.unlink(thefile)
      except:
        print "Could not unlink file %s: %s - %s" % (thefile, sys.exc_info()[0], sys.exc_info()[1])
  else:
    print "File %s is not on sufficient tapes to be elligable for deletion" % thefile
