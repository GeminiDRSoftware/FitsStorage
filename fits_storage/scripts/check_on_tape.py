import sys
import datetime
import urllib.request, urllib.parse, urllib.error
from xml.dom.minidom import parseString
from sqlalchemy import join
from fits_storage.orm import session_scope
from gemini_obs_db.file import File
from gemini_obs_db.diskfile import DiskFile
from fits_storage.logger import logger, setdebug, setdemon


if __name__ == "__main__":

    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--tapeserver", action="store", type="string", dest="tapeserver", default="hbffitstape1", help="The Fits Storage Tape server to use to check the files are on tape")
    parser.add_option("--file-pre", action="store", type="string", dest="filepre", help="File prefix to operate on, eg N20090130, N200812 etc")
    parser.add_option("--only", action="store_true", dest="only", help="Only list files that need to go to tape")
    parser.add_option("--notpresent", action="store_true", dest="notpresent", help="Include files that are marked as not present")
    parser.add_option("--mintapes", action="store", type="int", dest="mintapes", default=2, help="Minimum number of tapes file must be on to be eligable for deletion")
    parser.add_option("--tapeset", action="store", type="int", dest="tapeset", help="Only consider tapes in this tapeset")
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

    (options, args) = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)


    msg = ""
    # Annouce startup
    logger.info("*********    check_on_tape.py - starting up at %s" % datetime.datetime.now())

    with session_scope() as session:
        query = session.query(DiskFile).select_from(join(File, DiskFile)).filter(DiskFile.canonical==True)

        if options.filepre:
            likestr = "%s%%" % options.filepre
            query = query.filter(File.name.like(likestr))

        if not options.notpresent:
            query = query.filter(DiskFile.present==True)

        query = query.order_by(File.name)

        cnt = query.count()
        if cnt == 0:
            logger.info("No Files found matching file-pre. Exiting")
            sys.exit(0)

        logger.info("Got %d files to check" % cnt)

        sumbytes = 0
        sumfiles = 0

        for diskfile in query:
            fullpath = diskfile.fullpath()
            dbmd5 = diskfile.file_md5
            dbfilename = diskfile.filename

            url = "http://%s/fileontape/%s" % (options.tapeserver, dbfilename)
            logger.debug("Querying tape server DB at %s" % url)

            xml = urllib.request.urlopen(url).read()
            dom = parseString(xml)
            fileelements = dom.getElementsByTagName("file")

            tapeids = []
            for fe in fileelements:
                filename = fe.getElementsByTagName("filename")[0].childNodes[0].data
                md5 = fe.getElementsByTagName("md5")[0].childNodes[0].data
                tapeid = int(fe.getElementsByTagName("tapeid")[0].childNodes[0].data)
                tapeset = int(fe.getElementsByTagName("tapeset")[0].childNodes[0].data)
                logger.debug("Filename: %s; md5=%s, tapeid=%d, tapeset=%d" % (filename, md5, tapeid, tapeset))
                if (filename == dbfilename) and (md5 == dbmd5) and (tapeid not in tapeids):
                    logger.debug("Found it on tape id %d" % tapeid)
                    if options.tapeset is not None and tapeset != options.tapeset:
                        logger.debug("But this tape id is not in the requested tapeset")
                    else:
                        tapeids.append(tapeid)

            if len(tapeids) < options.mintapes:
                sumbytes += diskfile.file_size
                sumfiles += 1
                logger.info("*** File %s - %s needs to go to tape, it is on %d tapes: %s" % (fullpath, dbmd5, len(tapeids), tapeids))
            else:
                if not options.only:
                    logger.info("File %s - %s is OK, it already is on %d tapes: %s" % (fullpath, dbmd5, len(tapeids), tapeids))

        logger.info("Found %d files totalling %.2f GB that should go to tape" % (sumfiles, sumbytes/1.0E9))
        logger.info("**check_on_tape.py exiting normally")
