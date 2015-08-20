import sys
import datetime
import urllib
from xml.dom.minidom import parseString
import os
import smtplib
from sqlalchemy import join, desc

from fits_storage.orm import session_scope
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.file import File
from fits_storage.fits_storage_config import storage_root, target_max_files, target_gb_free
from fits_storage.logger import logger, setdebug, setdemon


# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--tapeserver", action="store", type="string", dest="tapeserver", default="hbffitstape1", help="The Fits Storage Tape server to use to check the files are on tape")
parser.add_option("--path", action="store", type="string", dest="path", default="", help="Path within the storage root")
parser.add_option("--file-pre", action="store", type="string", dest="filepre", help="File prefix to operate on, eg N20090130, N200812 etc")
parser.add_option("--maxnum", type="int", action="store", dest="maxnum", help="Delete at most X files.")
parser.add_option("--maxgb", type="float", action="store", dest="maxgb", help="Delete at most X GB of files")
parser.add_option("--auto", action="store_true", dest="auto", help="Delete old files to get to pre-defined free space")
parser.add_option("--oldbyfilename", action="store_true", dest="oldbyfilename", help="Sort by filename to determine oldest files")
parser.add_option("--oldbylastmod", action="store_true", dest="oldbylastmod", help="Sort by lastmod to determine oldest files")
parser.add_option("--numbystat", action="store_true", dest="numbystat", default=False, help="Use statvfs rather than database to determine number of files on the disk")
parser.add_option("--yesimsure", action="store_true", dest="yesimsure", help="Needed when file count is large")
parser.add_option("--notpresent", action="store_true", dest="notpresent", help="Include files that are marked as not present")
parser.add_option("--mintapes", action="store", type="int", dest="mintapes", default=2, help="Minimum number of tapes file must be on to be eligable for deletion")
parser.add_option("--skip-md5", action="store_true", dest="skipmd5", help="Do not bother to verify the md5 of the file on disk")
parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Dry Run - do not actually do anything")
parser.add_option("--emailto", action="store", type="string", dest="emailto", help="Email address to send message to")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)


msglines = []
# Annouce startup
logger.info("*********    delete_files.py - starting up at %s" % datetime.datetime.now())

def add_to_msg(log, msg):
    log(msg)
    msglines.append(msg)

with session_scope() as session:
    query = session.query(DiskFile).select_from(join(File, DiskFile)).filter(DiskFile.canonical==True)

    if options.auto:
        # chdir to the storage root to kick the automounter
        cwd = os.getcwd()
        os.chdir(storage_root)
        s = os.statvfs(storage_root)
        os.chdir(cwd)
        gbavail = s.f_bsize * s.f_bavail / (1024 * 1024 * 1024)
        if options.numbystat:
            numfiles = s.f_files - s.f_favail
        else:
            numfiles = session.query(DiskFile).filter(DiskFile.present == True).count()
        logger.debug("Disk has %d files present and %.2f GB available" % (numfiles, gbavail))
        numtodelete = numfiles - target_max_files
        if numtodelete > 0:
            add_to_msg(logger.info, "Need to delete at least %d files" % numtodelete)

        gbtodelete = target_gb_free - gbavail
        if gbtodelete > 0:
            add_to_msg(logger.info, "Need to delete at least %.2f GB" % gbtodelete)

        if numtodelete <= 0 and gbtodelete <= 0:
            logger.info("In Auto mode and nothing needs deleting. Exiting")
            sys.exit(0)

    if options.filepre:
        likestr = "%s%%" % options.filepre
        query = query.filter(File.name.like(likestr))

    if not options.notpresent:
        query = query.filter(DiskFile.present == True)

    if options.oldbylastmod:
        query = query.order_by(desc(DiskFile.lastmod))
    else:
        query = query.order_by(File.name)

    if options.maxnum:
        query = query.limit(options.maxnum)

    cnt = query.count()

    if cnt == 0:
        logger.info("No Files found matching file-pre. Exiting")
        sys.exit(0)

    logger.info("Got %d files to consider for deletion" % cnt)
    if cnt > 2000 and not options.yesimsure:
        logger.error("To proceed with this many files, you must say --yesimsure")
        sys.exit(1)

    sumbytes = 0
    sumfiles = 0
    sumgb = 0

    for diskfile in query:
        badmd5 = False

        fullpath = diskfile.fullpath()
        dbmd5 = diskfile.file_md5
        dbfilename = diskfile.filename

        logger.debug("Full path filename: %s" % fullpath)
        if not diskfile.exists():
            logger.error("Cannot access file %s" % fullpath)
        else:

            if not options.skipmd5:
                filemd5 = diskfile.get_file_md5()
                logger.debug("Actual File MD5 and canonical database diskfile MD5 are: %s and %s" % (filemd5, dbmd5))
                if filemd5 != dbmd5:
                    logger.error("File: %s has an md5sum mismatch between the database and the actual file. Skipping" % dbfilename)
                    badmd5 = True
            else:
                filemd5 = dbmd5

            if not badmd5:
                url = "http://%s/fileontape/%s" % (options.tapeserver, dbfilename)
                logger.debug("Querying tape server DB at %s" % url)

                xml = urllib.urlopen(url).read()
                dom = parseString(xml)
                fileelements = dom.getElementsByTagName("file")

                tapeids = []
                for fe in fileelements:
                    filename = fe.getElementsByTagName("filename")[0].childNodes[0].data
                    md5 = fe.getElementsByTagName("md5")[0].childNodes[0].data
                    tapeid = int(fe.getElementsByTagName("tapeid")[0].childNodes[0].data)
                    logger.debug("Filename: %s; md5=%s, tapeid=%d" % (filename, md5, tapeid))
                    found = (filename == dbfilename) and (tapeid not in tapeids)
                    if not options.skipmd5:
                        found = found and (md5 == filemd5)

                    if found:
                        logger.debug("Found it on tape id %d" % tapeid)
                        tapeids.append(tapeid)

                if len(tapeids) >= options.mintapes:
                    sumbytes += diskfile.file_size
                    sumgb = sumbytes / 1.0E9
                    sumfiles += 1
                    if options.dryrun:
                        add_to_msg(
                            logger.info,
                            "Dry run - not actually deleting File %s - %s which is on %d tapes: %s" % (fullpath, filemd5, len(tapeids), tapeids)
                            )
                    else:
                        add_to_msg(
                            logger.info,
                            "Deleting File %s - %s which is on %d tapes: %s" % (fullpath, filemd5, len(tapeids), tapeids)
                            )
                        try:
                            os.unlink(fullpath)
                            logger.debug("Marking diskfile id %d as not present" % diskfile.id)
                            diskfile.present = False
                            session.commit()
                        except:
                            add_to_msg(
                                logger.error,
                                "Could not unlink file %s: %s - %s" % (fullpath, sys.exc_info()[0], sys.exc_info()[1])
                                )
                else:
                    add_to_msg(
                        logger.info,
                        "File %s is not on sufficient tapes to be elligable for deletion" % dbfilename
                        )
            if options.maxgb:
                if sumgb>options.maxgb:
                    add_to_msg(logger.info, "Allready deleted %.2f GB - stopping now" % sumgb)
                    break
            if options.auto:
                if (numtodelete > 0) and (sumfiles >= numtodelete):
                    add_to_msg(
                        logger.info,
                        "Have now deleted the necessary number of files: %d Stopping now" % sumfiles
                        )
                    break
                if (gbtodelete > 0) and (sumgb >= gbtodelete):
                    add_to_msg(
                        logger.info,
                        "Have now deleted the necessary number of GB: %.2f Stopping now" % sumgb
                        )
                    break

if options.emailto:
    if options.dryrun:
        subject = "Dry run file delete report"
    else:
        subject = "File delete report"

    mailfrom = 'fitsdata@gemini.edu'
    mailto = [options.emailto]

    message = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" % (mailfrom, ", ".join(mailto), subject, '\n'.join(msglines))

    server = smtplib.SMTP('mail.gemini.edu')
    server.sendmail(mailfrom, mailto, message)
    server.quit()

logger.info("**delete_files.py exiting normally")
