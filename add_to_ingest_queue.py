import sys
sys.path=['/opt/sqlalchemy/lib/python2.5/site-packages', '/astro/iraf/x86_64/gempylocal/lib/stsci_python/lib/python2.5/site-packages']+sys.path

import FitsStorage
from FitsStorageUtils import *
import os
import re
import datetime
import time

from optparse import OptionParser

parser = OptionParser()
parser.add_option("--file-re", action="store", type="string", dest="file_re", help="python regular expression string to select files by. Special values are today, twoday, fourday to include only files from today, the last two days, or the last four days respectively (days counted as UTC days)")
(options, args) = parser.parse_args()

# Annouce startup
now = datetime.datetime.now()
startup = "*********  add_to_ingest_queue.py - starting up at %s" % now
print "\n\n%s\n" % startup

# Get a list of all the files in the datastore
# We assume this is just one dir (ie non recursive) for now.
path=''
fulldirpath = os.path.join(FitsStorage.storage_root, path)
print "Ingesting files from: ", fulldirpath

file_re = options.file_re
# Handle the today and twoday options
now=datetime.datetime.utcnow()
delta=datetime.timedelta(days=1)
if(options.file_re == "today"):
  file_re=now.date().strftime("%Y%m%d")

if(options.file_re == "twoday"):
  then=now-delta
  a=now.date().strftime("%Y%m%d")
  b=then.date().strftime("%Y%m%d")
  file_re="%s|%s" % (a, b)

if(options.file_re == "fourday"):
  a=now.date().strftime("%Y%m%d")
  then=now-delta
  b=then.date().strftime("%Y%m%d")
  then=then-delta
  c=then.date().strftime("%Y%m%d")
  then=then-delta
  d=then.date().strftime("%Y%m%d")
  file_re="%s|%s|%s|%s" % (a, b, c, d)

filelist = os.listdir(fulldirpath)

cre=re.compile(file_re)

files=[]
if(file_re):
  for filename in filelist:
    if(cre.search(filename)):
      files.append(filename)
else:
  files = filelist


i=0
n=len(files)
# print what we're about to do, and give abort opportunity
print "About to scan %d files" % n
if (n>5000):
  print "That's a lot of files. Hit ctrl-c within 5 secs to abort"
  time.sleep(6)

session = sessionfactory()

for filename in files:
  i+=1
  print "-- Inserting (%d/%d): %s" % (i, n, filename)
  addto_ingestqueue(session, filename, path)

session.close()
now=datetime.datetime.now()
print "*** add_to_ingestqueue.py exiting normally at %s" % now

