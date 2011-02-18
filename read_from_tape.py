import sys
sys.path=['/opt/sqlalchemy/lib/python2.5/site-packages', '/astro/iraf/x86_64/gempylocal/lib/stsci_python/lib/python2.5/site-packages']+sys.path

import FitsStorage
from FitsStorageConfig import *
from FitsStorageLogger import *
from FitsStorageUtils import *
from FitsStorageTape import TapeDrive
import CadcCRC
import os
import re
import datetime
import time
import subprocess
import tarfile
import urllib
from xml.dom.minidom import parseString


# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--tapedrive", action="store", type="string", dest="tapedrive", help="tapedrive to use.")
parser.add_option("--file-re", action="store", type="string", dest="filere", help="Regular expression used to select files to extract")
parser.add_option("--all", action="store_true", dest="all", help="When multiple versions of a file are on tape, get them all, not just the most recent")
parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Dry Run - do not actually do anything")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
logger.info("*********  read_from_tape.py - starting up at %s" % datetime.datetime.now())
options.filere = 'N2009101'
options.tapedrive = 1

if(not options.filere):
  logger.error("You must specify a file-re")
  sys.exit(1)

if(not options.tapedrive):
  logger.error("You must specify a tape drive")
  sys.exit(1)

# Query the DB to find a list of files to extract
# This is a little non trivial, given that there are multiple identical
# copies of the file on several tapes and also that there can be multiple
# non identical version of the file on tapes too.
session = sessionfactory()

# First, we get a list of filename, md5 pairs for the files we want to extract
query = session.query(TapeFile.filename, TapeFile.md5, Tape.id).select_from(join(TapeFile, join(TapeWrite, Tape)))
query = query.filter(TapeFile.filename.like('%'+options.filere+'%'))
query = query.filter(TapeWrite.suceeded == True)
query = query.filter(Tape.active == True)
query = query.order_by(TapeFile.filename, desc(TapeFile.lastmod))
query = query.distinct().all()

# Now, where the same filename occurs with multiple md5s, we should weed out the ones we don't want
todolist = []
todolist2 = []
filenames = []
previous_file = ''
if(not options.all):
  for que in query:
    this_file = que.filename
    if(previous_file != this_file):
      todolist.append(que)
      todolist2.append(que)
      filenames.append(que[0])
    previous_file = this_file
  print "count: %d" % len(todolist)

# need a todolist of (file, md5) left to read, that we can whittle down as we go
while(len(todolist)):
  tapeslist = []
  while(len(todolist)):
    todo = todolist.pop()
    # If the next item is on a tape that is already listed in tapeslist, then we don't query it!
    if todo[2] not in tapeslist:
      tapes = session.query(Tape.id).select_from(join(TapeFile, join(TapeWrite, Tape))).filter(TapeFile.filename.like('%s' % todo[0])).all()
      print "tapes: %s" % tapes
      for t in tapes:
        tapeslist.append(t[0])

  print 'tapes: %s' % sorted(tapeslist)
  it = raw_input('Which tape would you like to read? ')

  # Query for all the rows from the selected tape
  onetape = session.query(TapeFile.filename, TapeFile.md5, Tape.id).select_from(join(TapeFile, join(TapeWrite, Tape)))
  onetape = onetape.filter(TapeFile.filename.like('%'+options.filere+'%'))
  onetape = onetape.filter(TapeWrite.suceeded == True)
  onetape = onetape.filter(Tape.id == it)
  onetape = onetape.distinct().all()

  # Reducing the todolist of filenames by removing the files that were read on the tape selected
  for one in onetape:
    if one[0] in filenames:
      filenames.remove(one[0])

  # Must repopulate todolist...
  counttodo = 0
  for todo2 in todolist2:
    if todo2[0] in filenames:
      todolist.append(todo2)
      counttodo += 1
  print "todolist: %s" % len(todolist)


print "No files in the todolist are unread."
# Verify (file, md5) read OK and if so, delete it from the todolist

session.close()

