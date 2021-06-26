#!/usr/bin/env python

"""Versions Resolver

Usage:
  resolve_datafile_versions.py process [--uniques] [--verbose] [--dryrun] [--debug] [--demon]
  resolve_datafile_versions.py parallel [--server=ADDR] [--verbose] [--dryrun] [--debug] [--demon]
  resolve_datafile_versions.py scan <directory>... [--noidemp] [--dryrun] [--debug] [--demon]
  resolve_datafile_versions.py reset (able | <filename>... [--server=ADDR]) [--dryrun] [--debug]
  resolve_datafile_versions.py rename [--threshold=THR] [--dryrun] [--debug] [--demon]
  resolve_datafile_versions.py (-h | --help)
  resolve_datafile_versions.py --version

Options:
  -h --help                Show this screen.
  -D, --debug              Increase log level to debug.
  -d, --demon              Run as a background demon, do not generate stdout
  -r, --dryrun             Don't actually do anything, just say what would be done.
  -s ADDR, --server=ADDR   Server network address of the redis server [default: localhost].
  -t THR, --threshold=THR  Check if files with score THR or less need renaming [default: -100]
  -u, --uniques            Look for unique files and mark them as accepted. May take long time.
  -v, --verbose            Display messages explaining the scoring decision

"""
import os
import sys
import bz2
import itertools

from operator import attrgetter, itemgetter
# from optparse import OptionParser
from docopt import docopt

from astropy.io import fits as pf

from fits_storage.orm.resolve_versions import Version
from fits_storage.orm.tapestuff import Tape, TapeWrite, TapeFile

from fits_storage.utils import resolve_scoring

from fits_storage.logger import logger, setdebug, setdemon
import datetime

# parser = OptionParser()
# parser.add_option("--dryrun",   action="store_true", dest="dryrun", default=False, help="Don't actually do anything, just say what would be done")
# parser.add_option("--srcdir",   action="store", dest="srcdir", default="/sdata/all_gemini_data/from_tape", help="Source directory to pull files from")
# parser.add_option("--destdir",  action="store", dest="destdir", default="/sdata/all_gemini_data/canonical", help="Destination directory to put files in")
# parser.add_option("--scan",     action="store_true", default=False, dest="scan", help="Scan directories to DB")
# parser.add_option("--noidemp",  action="store_true", default=False, dest="noidemp", help="When scanning, assume that we start with a blank database and don't worry about inserting duplicates")
# parser.add_option("--able",     action="store_true", default=False, dest="able", help="Reset all unable flags to False")
# parser.add_option("--debug",    action="store_true", dest="debug", help="Increase log level to debug")
# parser.add_option("--uniques",  action="store_true", default=False, dest="uniq", help="Look for truly, unique files and mark them as accepted")
# parser.add_option("--demon",    action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")
# parser.add_option("--parallel", action="store_true", dest="parallel", help="Run just the de-duplicating, taking the names from a redis server")
# parser.add_option("--server",   action="store", dest="server", default='localhost', help="Server network address for --parallel")

arguments = docopt(__doc__, version='Versions Resolver 2.0')
# print(arguments)
# sys.exit(0)


def scan_directory(sess, which, idemp):
    # Recurse through the directory structure, finding files, add to db.
    for root, dirs, files in os.walk(which):
        logger.debug("Walking root %s, found %d dirs and %d files", root, len(dirs), len(files))
        for file in files:
            fullpath = os.path.join(root, file)
            if ('fits' in file) and (not file.startswith('.')):
                # This operation should be idempotent. If we can find the full path in the database,
                # then skip it! Unless user says otherwise
                if (not idemp) or (sess.query(Version.id).filter(Version.fullpath == fullpath).count() == 0):
                    logger.info("Found %s at %s", file, fullpath)
                    version = Version(file, fullpath)
                    sess.add(version)
            else:
                logger.info("Ignoring non fits file %s", file)
        sess.commit()

def resolve_uniques(sess):
    # This is a subquery that selects the ids of the unique filenames
    sq = sess.query(orm.func.min(Version.id).label('mid')).\
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
    rc = sess.execute(stmt).rowcount
    if rc:
        logger.info("- Found %s such files. Now marked as accepted", rc)
    else:
        logger.info("- No unique, non-accepted files found")

# De-duplicate...
def deduplicate(sess, fname):
    versions = list(sess.query(Version).\
                         filter(Version.filename == fname).\
                         filter(Version.is_clear == None).\
                         filter(Version.accepted == None))

    # Calculate scoring
    versdict = {}
    for vers in versions:
        versdict[vers.fullpath] = vers
    for path, score in image_validity.score_files(*list(versdict.keys()), verbose = verb):
        vers = versdict[path]
        vers.score = score.value
        # Something really bad happened here...
        if score.value < -9000:
            vers.unable = True
            vers.is_clear = True
    # Cleanup...
    versdict = None
    if all(x.unable for x in versions):
        # No valid version left
        sess.commit()
        return

    versions = sorted(versions, key=attrgetter('score'), reverse = True)
    max_val = versions[0].score
    for vers in versions[1:]:
        if vers.score < max_val:
            vers.accepted = False
            vers.is_clear = False

    candidates = [x for x in versions if x.score == max_val]

    if len(candidates) > 1:
        logger.info("{0}: Found more than one max score. Trying MD5".format(fname))

        for vers in candidates:
            vers.calc_md5()
        # MD5 matching. Rejects all exact copies except for one
        for (md5, group) in itertools.groupby(candidates, attrgetter('data_md5')):
            for inst in list(group)[1:]:
                inst.accepted = False
                inst.is_clear = True

        best = [x for x in candidates if x.accepted != False]
        if len(best) > 1:
            # Last chance. Let's check the modification date
            # Assume that fname ends with .bz2

            # Log the case...
            open('/data/differences/last_resort.log', 'a').write('{0}\n'.format(fname))

            valid_paths = tuple(x.fullpath[x.fullpath.find('Gemini_FITS'):] for x in best)
            nobz2 = os.path.splitext(fname)[0]
            stamps = sess.query(Tape.label, TapeWrite.filenum, TapeFile.filename, TapeFile.lastmod).\
                             join(TapeWrite, TapeFile).\
                             filter(orm.func.lower(TapeFile.filename) == nobz2.lower())
            interesting = [xx for xx in ((x[-1], x[0] + '-' + str(x[1]) + '/' + x[2] + '.bz2') for x in stamps) if xx[1] in valid_paths]
            if interesting:
                by_most_recent = sorted(interesting, key = itemgetter(0), reverse = True)
                path_to_winner = by_most_recent[0][1]

                logger.info("  - Had to resort to last modification")
                for inst in best:
                    inst.accepted = (True if inst.fullpath.endswith(path_to_winner) else False)
                    inst.is_clear = False
                    inst.used_date = True
            else:
                logger.info("  - Still undecided. Marking the potential winners as unable")
                for vers in best:
                    vers.unable = True

        else:
            winner = best[0]
            winner.accepted = True
            winner.is_clear = (True if len(candidates) == len(versions) else False)
    else:
        logger.info("{0}: Found a winner with score {1}".format(fname, max_val))
        winner = candidates[0]
        winner.accepted = True
        winner.is_clear = False

    sess.commit()


if __name__ == "__main__":

    # Logging level to debug? Include stdio log?
    setdebug(arguments['--debug'])
    setdemon(arguments['--demon'])

    # Annouce startup
    logger.info("*********    resolve_datafile_versions.py - starting up at %s" % datetime.datetime.now())

    # Get a session
    with orm.session_scope() as session:
        if arguments['scan']:
            idemp = not arguments['--noidemp']
            for srcdir in arguments['<directory>']:
                scan_directory(session, srcdir, idemp)
            logger.info("Exiting after scan")
            sys.exit(0)

        def reset_version(vers):
            vers.unable = False
            vers.score = -1
            vers.accepted = None
            vers.is_clear = None
            vers.used_date = None

        if arguments['reset']:
            if arguments['able']:
                logger.info("Setting unable=False on all entries")
                session.execute("UPDATE versions SET unable=False")
                session.commit()
            else:
                from redis import Redis
                found = set()
                for vers in session.query(Version).filter(Version.filename.in_(arguments['<filename>'])):
                    found.add(vers.filename)
                    reset_version(vers)
                session.commit()

                r = Redis(arguments['--server'])
                for fname in found:
                    r.lpush('pending', fname)
            sys.exit(0)

        def fix_header(value):
            try:
                if value.startswith('='):
                    return value.split("'")[1].strip()
            except AttributeError:
                value = 'UNKNOWN'

            return value

        if arguments['rename']:
            thr = arguments['--threshold']
            query = session.query(Version.filename).\
                            filter(Version.accepted == True).\
                            filter(Version.score <= thr).\
                            group_by(Version.filename)

            for (fname,) in query:
                logger.info("Looking up {0}".format(fname))
                versions = list(session.query(Version).filter(Version.filename == fname))
                headers = []
                passed = []
                instrs = []
                for v in versions:
                    try:
                        fits = pf.open(bz2.BZ2File(v.fullpath))
                        fits.verify('silentfix+exception')
                        passed.append(v)
                        headers.append(fits[0].header)
                    except pf.verify.VerifyError:
                        pass
                instrs = [fix_header(x.get('INSTRUME')) for x in headers]
                if len(set(instrs)) > 1:
                    logger.info(" - Renaming")
                    for v, instr in zip(passed, instrs):
                        reset_version(v)
                        exploded = v.filename.split('.')
                        new_fname = '.'.join([exploded[0], instr] + exploded[1:])
                        v.filename = new_fname
                    session.commit()
            sys.exit(0)

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
        if arguments['process']:
            ################################################################################
            # Look for filenames that appear just once in the database and mark them as
            # accepted (and clear).

            if arguments['--uniques']:
                logger.info("Looking for purely unique filenames...")
                resolve_uniq(session)
                session.commit()

            logger.info("De-duplicating: scoring + MD5")
            logger.info("Starting sequential process: will query the database for files")
            query = session.query(Version.filename).\
                            filter(Version.unable == False).\
                            filter(Version.is_clear == None).\
                            filter(Version.accepted == None).\
                            group_by(Version.filename)

            for (fname,) in query:
                deduplicate(session, fname)
        elif arguments['parallel']:
            logger.info("De-duplicating: scoring + MD5")
            logger.info("Starting a parallel process: will query Redis for files")
            from redis import Redis

            verb = arguments['--verbose']
            r = Redis(arguments['--server'])

            fname = r.lpop('pending')
            try:
                while fname is not None:
                    deduplicate(session, fname)
                    fname = r.lpop('pending')
            finally:
                if fname is not None:
                    r.lpush('pending', fname)
