from FitsStorage import *
from FitsStorageLogger import *
from optparse import OptionParser

parser = OptionParser()
parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run as a background demon, do not generate stdout")
parser.add_option("--txtfile", action="store", dest="txtfile", help="Input file to read")
(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
now = datetime.datetime.now()
logger.info("*********  notification_ingest.py - starting up at %s" % now)

session = sessionfactory()
try:

  f = open(options.txtfile, 'r')

  for line in f:
    things = line.split(' ')

    projectid = things[0]

    n = Notification('Auto %s' % projectid)
    n.selection = '%s/science' % projectid
    for i in [2,3,4]:
      if(things[i] == 'none'):
        things[i]=''
    n.to = things[1]
    n.cc = ','.join(things[2:])
    n.internal = False
  
    logger.info("Adding %s - %s: %s - %s" % (n.label, n.selection, n.to, n.cc))

    session.add(n)
    session.commit()

  f.close()


finally:
  session.close()

session.close()
logger.info("*********  notifcation_ingest.py - exiting at %s" % datetime.datetime.now())

