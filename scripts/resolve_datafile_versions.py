import bz2
import pyfits
from optparse import OptionParser
import os
import sys

from orm import sessionfactory
from orm.resolve_versions import Version

from logger import logger, setdebug, setdemon
import datetime

parser = OptionParser()
parser.add_option("--dryrun", action="store_true", dest="dryrun", default=False, help="Don't actually do anything, just say what would be done")
parser.add_option("--srcdir", action="store", dest="srcdir", default="/sdata/all_gemini_data/from_tape", help="Source directory to pull files from")
parser.add_option("--destdir", action="store", dest="destdir", default="/sdata/all_gemini_data/canonical", help="Destination directory to put files in")
parser.add_option("--scan", action="store_true", default=False, dest="scan", help="Scan directories to DB")
parser.add_option("--able", action="store_true", default=False, dest="able", help="Reset all unable flags to False")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")


(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
logger.info("*********    resolve_datafile_versions.py - starting up at %s" % datetime.datetime.now())

# Get a session
session = sessionfactory()

if options.scan:
    # Recurse through the directory structure, finding files, add to db.
    for root, dirs, files in os.walk(options.srcdir):
        logger.debug("Walking root %s, found %d dirs and %d files", root, len(dirs), len(files))
        for file in files:
            fullpath = os.path.join(root, file)
            logger.info("Found %s at %s", file, fullpath)
            if 'fits' in file:
                version = Version(file, fullpath)
                session.add(version)
            else:
                logger.info("Ignoring non fits file %s", file)
        session.commit()
    logger.info("Exiting after scan")
    sys.exit(0)

if options.able:
    logger.info("Setting unale=False on all entries")
    session.execute("UPDATE versions SET unable=False")
    session.commit()

# Itterate through the entries. First pass
done = False
while not done:
    query = session.query(Version).filter(Version.unable == False)
    reference = query.first()
    if reference is None:
        done = True
        break

    # Is it a purely unique filename?
    query = session.query(Version).filter(Version.unable == False)
    query = query.filter(Version.id != reference.id)
    query = query.filter(Version.filename == reference.filename)
    others = query.all()

    if len(others) == 0:
        # Yes, it's a purely unique filename. Get it out of the way.
        logger.info("%s is a unique filename", reference.filename)
        if not options.dryrun:
            reference.moveto(options.destdir)
            session.delete(reference)
            session.commit()
        break
    else:
        logger.debug("%s is not unique: %d more versions", reference.filename, len(others))

# Now go through what's left and fill in md5s.
logger.info("Calculating md5s")
done = False
while not done:
    query = session.query(Version).filter(Version.unable == False).filter(Version.data_md5 == None)
    reference = query.first()
    if reference:
        logger.info("Calculating MD5 for %s", reference.fullpath)
        reference.calc_md5()
        session.commit()
    else:
        done = True

# Now look for duplicates
logger.info("De-duplicating.")
done = False
while not done:
    query = session.query(Version).filter(Version.unable == False)
    reference = query.first()
    if reference is None:
        done = True
        break

    # find any identical others - ie duplicate files, and get rid of them.
    query = session.query(Version).filter(Version.unable == False)
    query = query.filter(Version.id != reference.id)
    query = query.filter(Version.filename == reference.filename)
    query = query.filter(Version.data_md5 == reference.data_md5)
    others = query.all()

    for other in others:
        # This one is identical to the reference, delete it
        logger.info("%s is identical to %s", other.fullpath, reference.fullpath)
        if not options.dryrun:
            os.unlink(other.fullpath)
        session.delete(other)
    session.commit() 

    # Find any others
    query = session.query(Version).filter(Version.unable == False)
    query = query.filter(Version.filename == reference.filename)
    others = query.all()

    # Is it (now) a purely unique filename?
    if len(others) == 0:
        # Yes, it's a purely unique filename
        logger.info("%s is a unique filename", reference.filename)
        if not options.dryrun:
            reference.moveto(options.destdir)
            session.delete(reference)
            session.commit()
        break
    else:
        # We're unable to resolve this 
        reference.unable = True
        session.commit()


session.close()
