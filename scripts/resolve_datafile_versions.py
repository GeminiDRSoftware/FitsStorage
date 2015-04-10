import bz2
import pyfits
from optparse import OptionParser
import os
import sys

import orm
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
session = orm.sessionfactory()

if options.scan:
    # Recurse through the directory structure, finding files, add to db.
    for root, dirs, files in os.walk(options.srcdir):
        logger.debug("Walking root %s, found %d dirs and %d files", root, len(dirs), len(files))
        logger.info("Walking root %s...", root)
        for file in files:
            fullpath = os.path.join(root, file)
            if ('fits' in file) and (not file.startswith('.')):
                # This operation should be idempotent. If we can find the full path in the database,
                # then skip it!
                if session.query(Version.id).filter(Version.fullpath == fullpath).count() == 0:
                    logger.info("Found %s at %s", file, fullpath)
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

# First pass: look for filenames that appear just once in the
#             database. Move them away and remove from the table

logger.info("Looking for purely unique filenames...")

# This is a subquery that selects the ids of the unique filenames
sq = session.query(orm.func.min(Version.id).label('mid')).\
             filter(Version.accepted == None).\
             group_by(Version.filename).\
             having(orm.func.count(Version.filename) == 1).\
             subquery()

# And this will update the status of the filenames selected by 'sq'
stmt = orm.update(Version).\
           where(Version.id.in_(sq)).\
           values(accepted = True,
                  is_clear = True,
                  score    = 1)
rc = session.execute(stmt).rowcount
if rc:
    logger.info("- Found %s such files. Now marked as accepted", rc)
else:
    logger.info("- No unique, non-accepted files found")
session.commit()

# for n, obj in enumerate(query, 1):
#     logger.info("{} is unique filename".format(obj.filename))
#     if not options.dryrun:
#         vers = session.query(Version).filter(Version.id == obj.mid).one()
#         vers.accepted = True
#         vers.score = 1
#         # Batch the commits. Arbitrary number
#         if not (n % 10000):
#             session.commit()
# else:
#     session.commit()

# Now go through what's left and fill in md5s.
logger.info("Calculating md5s")
query = session.query(Version).\
                filter(Version.unable == False).\
                filter(Version.accepted == None).\
                filter(Version.data_md5 == None)
for n, reference in enumerate(query, 1):
    logger.info("Calculating MD5 for %s", reference.fullpath)
    if not options.dryrun:
        reference.calc_md5()
        # Batch the commits. Arbitrary number
        if not (n % 300):
            session.commit()
else:
    session.commit()

# De-duplicating rules
#
# First of all, we're going to look for exact duplicates, using the MD5 that we
# just calculted in the previous step. In those cases, we'll mark as accepted an
# arbitrary version.

# After the first, easy one, we'll go through a number or rules to "score" our
# files. The first approach will be naive: set the competing entries' scores to
# 0, and submit each one to a set of rules; the rule functions return a score,
# typically "0" for "not passed" or "1" for "passed", but the rules may award
# more than one point.
#
# The final score for one entry will be the sum of the result for all rules. At
# the end of the process, the version with the highest score is accepted. If
# there's a tie for highest score, we have failed in finding a winner and will
# mark all the involved versions as "unable".
#
# In a second iteration we should refine this process to make sure that no
# accepted version fails a test that others pass, just as an extra safety
# measure.

# Rules:
# 1) RAWGEMQA header - pick the file where != 'UNKNOWN'
# 2) RAWIQ, RAWCC, RAWWV, RAWBG headers - again, pick the one where != 'UNKNOWN'

# Now look for duplicates
# The method is simple: we group the files by filename and data_md5, and look
# for instances where there are more thank 1 of those. Then, for each filename
# that fits that condition, we query Version for all the instances of that filename.
# If all of them have the same MD5sum, we select a random one as the winner, and
# tag the others as rejected.
#
# In the case where some of the instances are exact duplicates, but not all of them,
# we choose one of the duplicates as partial winner, but WE DON'T MARK IT. We tag
# the others as rejected. Doing it like that, the "winner" will go into the next
# step, where it will compete with the non matching versions.
logger.info("De-duplicating: looking for exact duplicates")

query = session.query(Version.filename, Version.data_md5).\
                filter(Version.unable == False).\
                filter(Version.accepted == None).\
                filter(Version.data_md5 != None).\
                group_by(Version.filename).\
                group_by(Version.data_md5).\
                having(orm.func.count(Version.filename) > 1).\
                yield_per(10000)

for filename, md5sum in query:
    # We're taking here the reasonable assumption that all the instances share the
    # 'unable = False' feature
    res = session.query(Version).\
                  filter(Version.filename == filename).\
                  all()
    matching = [x for x in res if x.data_md5 == md5sum]
    if not options.dryrun:
        for inst in matching[1:]:
            inst.accepted = False
            inst.is_clear = True

    if res == matching:
        logger.info("Found exact duplicates for {0}. All accounted for".format(filename))
        if not options.dryrun:
            matching[0].accepted = True
            matching[0].is_clear = True
    else:
        logger.info("Found exact duplicates for {0}. Leaving one for next round".format(filename))
else:
    session.commit()

#while not done:
#    query = session.query(Version).filter(Version.unable == False)
#    reference = query.first()
#    if reference is None:
#        done = True
#        break
#
#    # find any identical others - ie duplicate files, and get rid of them.
#    query = session.query(Version).filter(Version.unable == False)
#    query = query.filter(Version.id != reference.id)
#    query = query.filter(Version.filename == reference.filename)
#    query = query.filter(Version.data_md5 == reference.data_md5)
#    others = query.all()
#
#    for other in others:
#        # This one is identical to the reference, delete it
#        logger.info("%s is identical to %s", other.fullpath, reference.fullpath)
#        if not options.dryrun:
#            os.unlink(other.fullpath)
#        session.delete(other)
#    session.commit()
#
#    # Find any others
#    query = session.query(Version).filter(Version.unable == False)
#    query = query.filter(Version.filename == reference.filename)
#    others = query.all()
#
#    # Is it (now) a purely unique filename?
#    if len(others) == 0:
#        # Yes, it's a purely unique filename
#        logger.info("%s is a unique filename", reference.filename)
#        if not options.dryrun:
#            reference.moveto(options.destdir)
#            session.delete(reference)
#            session.commit()
#        break
#    else:
#        # We're unable to resolve this 
#        reference.unable = True
#        session.commit()

session.close()
