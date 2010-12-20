import sys
sys.path=['/opt/sqlalchemy/lib/python2.5/site-packages', '/astro/iraf/x86_64/gempylocal/lib/stsci_python/lib/python2.5/site-packages']+sys.path

import FitsStorage
from FitsStorageConfig import *
from FitsStorageLogger import *
from FitsStorageUtils import *
import CadcCRC
import datetime


import urllib
from xml.dom.minidom import parseString


# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--tapeserver", action="store", type="string", dest="tapeserver", default="hbffitstape1", help="The Fits Storage Tape server to use to check the files are on tape")
parser.add_option("--path", action="store", type="string", dest="path", default="", help="Path within the storage root")
parser.add_option("--file-pre", action="store", type="string", dest="filepre", help="File prefix to operate on, eg N20090130, N200812 etc")
parser.add_option("--yesimsure", action="store_true", dest="yesimsure", help="Needed when file count is large")
parser.add_option("--notpresent", action="store_true", dest="notpresent", help="Include files that are marked as not present")
parser.add_option("--mintapes", action="store", type="int", dest="mintapes", default=2, help="Minimum number of tapes file must be on to be eligable for deletion")
parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Dry Run - do not actually do anything")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)


# Annouce startup
logger.info("*********  delete_files.py - starting up at %s" % datetime.datetime.now())

session = sessionfactory()
likestr = "%s%%" % options.filepre
query = session.query(DiskFile).select_from(join(File, DiskFile)).filter(File.filename.like(likestr)).filter(DiskFile.canonical==True)
if(not options.notpresent):
  query = query.filter(DiskFile.present==True)

query = query.order_by(desc(DiskFile.lastmod))

diskfiles = query.all()

if(len(diskfiles) == 0):
  logger.info("No Files found matching file-pre. Exiting")
  session.close()
  sys.exit(0)

logger.info("Got %d files to delete" % len(diskfiles))
if(len(diskfiles) > 1000 and not options.yesimsure):
  logger.error("To proceed with this many files, you must say --yesimsure")
  session.close()
  sys.exit(1)

for diskfile in diskfiles:

  logger.debug("Full path filename: %s" % diskfile.file.fullpath())
  if(not diskfile.file.exists()):
    logger.error("Cannot access file %s" % diskfile.file.fullpath())
    sys.exit(1)

  fileccrc = CadcCRC.cadcCRC(diskfile.file.fullpath())
  logger.debug("Actual File CCRC and canonical database diskfile CCRC are: %s and %s" % (fileccrc, diskfile.ccrc))
  if(fileccrc != diskfile.ccrc):
    logger.error("File: %s has a ccrc mismatch between the database and the actual file. Aborting." % diskfile.file.filename)
    session.close()
    exit(1)

  url = "http://%s/fileontape/%s" % (options.tapeserver, diskfile.file.filename)
  logger.debug("Querying tape server DB at %s" % url)

  u = urllib.urlopen(url)
  xml = u.read()
  u.close()

  dom = parseString(xml)

  fileelements = dom.getElementsByTagName("file")

  tapeids = []
  for fe in fileelements:
    filename = fe.getElementsByTagName("filename")[0].childNodes[0].data
    ccrc = fe.getElementsByTagName("ccrc")[0].childNodes[0].data
    tapeid = int(fe.getElementsByTagName("tapeid")[0].childNodes[0].data)
    logger.debug("Filename: %s; ccrc=%s, tapeid=%d" % (filename, ccrc, tapeid))
    if((filename == diskfile.file.filename) and (ccrc == fileccrc) and (tapeid not in tapeids)):
      logger.debug("Found it on tape id %d" % tapeid)
      tapeids.append(tapeid)

  logger.info("File %s - %s is on %d tapes: %s" % (diskfile.file.filename, fileccrc, len(tapeids), tapeids))
  if(len(tapeids) >= options.mintapes):
    if(options.dryrun):
      logger.info("Dry run - not actually deleting file %s" % diskfile.file.fullpath())
    else:
      logger.info("Deleting File %s" % diskfile.file.fullpath())
      try:
        os.unlink(diskfile.file.fullpath())
        logger.debug("Marking diskfile id %d as not present" % diskfile.id)
        diskfile.present = False
        session.commit()
      except:
        logger.error("Could not unlink file %s: %s - %s" % (diskfile.file.fullpath(), sys.exc_info()[0], sys.exc_info()[1]))
  else:
    logger.info("File %s is not on sufficient tapes to be elligable for deletion" % diskfile.file.filename)

session.close()
logger.info("**delete_files.py exiting normally")
