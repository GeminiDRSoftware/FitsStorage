import sys
sys.path=['/opt/sqlalchemy/lib/python2.5/site-packages', '/astro/iraf/x86_64/gempylocal/lib/stsci_python/lib/python2.5/site-packages']+sys.path

from FitsStorage import *
#import FitsStorageUtils
#import os
#import re
import datetime
#import time

from optparse import OptionParser

parser = OptionParser()
parser.add_option("--limit", action="store", type="int", help="specify a limit on the number of files to examine. The list is sorted by lastmod time before the limit is applied")

(options, args) = parser.parse_args()

# Annouce startup
now = datetime.datetime.now()
startup = "*********  rollcall.py - starting up at %s" % now
print "\n\n%s\n" % startup

# Get a list of all diskfile_ids marked as present
# If we try and really brute force a list of DiskFile objects, we run out of memory...
query = session.query(DiskFile.id).filter(DiskFile.present == True).order_by(DiskFile.lastmod)

# Did we get a limit option?
if(options.limit):
  query = query.limit(options.limit)

print "evaluating number of rows..."
n = query.count()
print "%d files to check" % n

# Semi Brute force approach for now. 
# It might be better to find some way to retrieve the items from the DB layer one at a time...
# If we try and really brute force a list of DiskFile objects, we run out of memory...
print "Getting list..."
list = query.all()
print "Starting checking..."

i=0
j=0
for dfid in list:
  # Search for it by ID (is there a better way?)
  df=session.query(DiskFile).filter(DiskFile.id == dfid[0]).one()
  if(not df.file.exists()):
    # This one doesn't actually exist
    df.present=False
    j+=1
    print "File %d/%d: Marking file %s (diskfile id %d) as not present" % (i, n, df.file.filename, df.id)
  else:
    if ((i % 1000) == 0):
      print "File %d/%d: present and correct" % (i, n)
  i+=1

print "\nMarked %d files as no longer present\n" % j

now=datetime.datetime.now()
print "*** rollcall.py exiting normally at %s" % now
