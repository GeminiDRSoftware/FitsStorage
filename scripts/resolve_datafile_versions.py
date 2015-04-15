from operator import attrgetter
from optparse import OptionParser
import itertools
import os
import sys

import orm
from orm.resolve_versions import Version

from utils import image_validity

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

################################################################################
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
sq = None    # Just destroying these two...
stmt = None

################################################################################
# De-duplicating rules
#
# We'll start by going through a number or rules to "score" our files.
# The first approach will be naive: set the competing entries' scores to
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
#
# To see the set of rules, look into ../utils/image_validity.py and look for
# functions decorated with @register_rule

# Now we iterate over the non-evaluated images using the following procedure:
#
#  1) We grab one filename
#  2) We query for all the non-evaluated instances of that filename
#  3) We score each instance using the image_validity scorer
#  4) We check if only one instance has the higher score.
#  4.1) If that's the case, we mark the instance as accepted and the others as
#       not, but always as not-clear
#  4.2) Otherwise, we calculate MD5sums on the high-scorers. If there are coincidences,
#       we mark all equal MD5sums (except for one) as "not-accepted, clear".
#  4.2.1) If there's only one instance left, we mark it as the winner.
#  4.2.2) Otherwise, we mark ALL instances as "unable"
#
# Note that this process CAN'T tag that "is_clear" as True, except in the case of
# exact duplicates, because we haven't at this point come up with an extensive set
# of rules
#
# If we can't declare a winner, we mark all the instances as "unable"
logger.info("De-duplicating: scoring + MD5")

query = session.query(Version.filename).\
                filter(Version.unable == False).\
                filter(Version.is_clear == None).\
                filter(Version.accepted == None).\
                group_by(Version.filename)

for (fname,) in query:
    versions = list(session.query(Version).\
                            filter(Version.filename == fname).\
                            filter(Version.is_clear == None).\
                            filter(Version.accepted == None))

    # Calculate scoring
    versdict = {}
    for vers in versions:
        versdict[vers.fullpath] = vers
    for path, score in image_validity.score_files(*versdict.keys()):
        vers = versdict[path]
        vers.score = score.value
        # Something really bad happened here...
        if score.value < -9000:
            vers.unable = True
    # Cleanup...
    versdict = None
    if all(x.unable for x in versions):
        # No valid version left
        session.commit()
        continue

    versions = sorted(versions, key=attrgetter('score'), reverse = True)
    max_val = versions[0].score
    for vers in versions[1:]:
        if vers.score < max_val:
            vers.accepted = False
            vers.is_clear = False

    candidates = filter(lambda x: x.score == max_val, versions)

    if len(candidates) > 1:
        logger.info("{0}: Found more than one max score. Trying MD5".format(fname))

        for vers in candidates:
            vers.calc_md5()
        # MD5 matching. Rejects all exact copies except for one
        for (md5, group) in itertools.groupby(candidates, attrgetter('data_md5')):
            for inst in list(group)[1:]:
                inst.accepted = False
                inst.is_clear = True

        winners = filter(lambda x: x.accepted != False, candidates)
        if len(winners) > 1:
            logger.info("  - Still undecided. Marking the potential winners as unable")
            for vers in winners:
                vers.unable = True
        else:
            winner = winners[0]
            winner.accepted = True
            winner.is_clear = (True if len(candidates) == len(versions) else False)
    else:
        logger.info("{0}: Found a winner with score {1}".format(fname, max_val))
        winner = candidates[0]
        winner.accepted = True
        winner.is_clear = False

    session.commit()

session.close()
