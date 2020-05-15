import sys
import os
import traceback
import datetime
import time
import shutil
import re
from abc import ABC, abstractmethod

from fits_storage.orm import session_scope
from fits_storage.orm.diskfile import DiskFile
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.scripts.header_fixer2 import fix_and_copy
from fits_storage.utils.ingestqueue import IngestQueueUtil

from fits_storage.fits_storage_config import using_s3, storage_root


# Utility functions
def check_present(session, filename):
    """
    Check if the given filename is present in the database.
    """
    # TODO this assumes path is a match, bad joojoo
    df = session.query(DiskFile).filter(DiskFile.filename==filename).filter(DiskFile.present==True).first()
    if df:
        return True
    return False


class VisitingInstrumentABC(ABC):
    """
    Base class for visiting instrument handling.

    This provides the common framework/structure and the
    implementations handle the peculiarities of each.
    """
    def __init__(self, base_path, apply_fixes):
        self.base_path = base_path
        self.apply_fixes = apply_fixes
    
    def check_filename(self, filename):
        return filename not in ['.bplusvtoc_internal', '.vtoc_internal']

    @abstractmethod
    def get_files(self):
        raise NotImplementedError("subclasses must implement get_files()")

    def prep(self):
        return
    
    @abstractmethod
    def get_destination(self, filename):
        raise NotImplementedError("subclasses must implement get_destination()")

    @abstractmethod
    def get_dest_path(self, filename):
        raise NotImplementedError("subclasses must implement get_dest_path()")

    def get_dest_filename(self, filename):
        return os.path.basename(filename)
    
    @abstractmethod
    def get_destination(self, filename):
        raise NotImplementedError("subclasses must implement get_dest_path()")

    def copy_over(self, session, iq, logger, filename, dryrun, force):
        src = os.path.join(self.base_path, filename)
        dst_filename = self.get_dest_filename(src)
        logger.debug("Calcuating dst_path from src=%s" % src)
        dst_path = self.get_dest_path(filename)
        logger.debug("Creating dst from %s %s %s" % (storage_root, dst_path, dst_filename))
        dst = os.path.join(storage_root, dst_path, dst_filename)
        dest = os.path.join(storage_root, self.get_destination(filename))
        # If the Destination file already exists, skip it
        if not force and os.access(dst, os.F_OK | os.R_OK):
            logger.info("%s already exists on storage_root - skipping", filename)
            return True
        # If the source path is a directory, skip is
        if os.path.isdir(src):
            logger.info("%s is a directory - skipping", filename)
            return True
        # If one of these wierd things, skip it
        if not self.check_filename(filename):
            logger.info("%s is a wierd thing - skipping", filename)
            return True
        # If lastmod time is within 5 secs, DHS may still be writing it. Skip it
        lastmod = datetime.datetime.fromtimestamp(os.path.getmtime(src))
        age = datetime.datetime.now() - lastmod
        age = age.total_seconds()
        if age < 5:
            logger.debug("%s is too new (%.1f)- skipping this time round", filename, age)
            return False
        else:
            logger.debug("%s age is OK: %.1f seconds", filename, age)
        # OK, try and copy the file over.
        try:
            if dryrun:
                logger.info("Dryrun - not actually copying %s", filename)
            else:
                logger.info("Copying %s to %s", filename, storage_root)
                # We can't use shutil.copy, because it preserves mode of the
                # source file, making the umask totally useless. Under the hood,
                # copy is just a shutil.copyfile + shutil.copymode. We'll
                # use copyfile instead.
                if not os.path.exists(os.path.join(storage_root, dst_path)):
                    os.mkdir(os.path.join(storage_root, dst_path))
                if self.apply_fixes:
                    fix_and_copy(os.path.dirname(src), os.path.join(storage_root, dst_path), dst_filename)
                else:
                    shutil.copyfile(src, dst)
                logger.info("Adding %s to IngestQueue", filename)
                
                iq.add_to_queue(dst_filename, dst_path, force=False, force_md5=False, after=None)
        except:
            logger.error("Problem copying %s to %s", src, storage_root)
            logger.error("Exception: %s : %s... %s", sys.exc_info()[0], sys.exc_info()[1],
                                                    traceback.format_tb(sys.exc_info()[2]))
            return False
        # Add it to the ingest queue here
        return True


class AlopekeZorroABC(VisitingInstrumentABC):
    def __init__(self, instr, path, apply_fixes):
        super().__init__(path, apply_fixes)
        self._instrument = instr

    def prep(self):
        if not os.path.exists(os.path.join(storage_root, self._instrument)):
            os.mkdir(os.path.join(storage_root, self._instrument))

    def get_files(self):
        for f in os.listdir(self.base_path):
            fullpath = os.path.join(self.base_path, f)
            if os.path.isdir(fullpath) and re.search(r'^\d{8}$', f):
                for datafile in os.listdir(fullpath):
                    if self._filename_re.search(datafile):
                        yield os.path.join(f, datafile)

    def get_destination(self, filename):
        return os.path.join(self._instrument, filename)

    def get_dest_path(self, filename):
        rel_path = os.path.split(filename)[0]
        return os.path.join(self._instrument, rel_path)


class Alopeke(AlopekeZorroABC):
    def __init__(self):
        super().__init__('alopeke', "/net/mkovisdata/home/alopeke/", True)
        self._filename_re = re.compile(r'N\d{8}A\d{4}[br].fits.bz2')
        self._filename_re = re.compile(r'S20200316Z\d{4}[br].fits.bz2')


class Zorro(AlopekeZorroABC):
    def __init__(self, base_path="/net/cpostonfs-nv1/tier2/ins/sto/zorro/"):
        super().__init__('zorro', base_path, True)
        self._filename_re = re.compile(r'S\d{8}Z\d{4}[br].fits.bz2')
        self._filename_re = re.compile(r'S202\d{5}Z\d{4}[br].fits.bz2')


class IGRINS(VisitingInstrumentABC):
    def __init__(self, base_path="/Users/ooberdorf/Downloads/IGRINS/"):
        super().__init__(base_path, True)

    def prep(self):
        if not os.path.exists(os.path.join(storage_root, 'igrins')):
            os.mkdir(os.path.join(storage_root, 'igrins'))

    def get_files(self):
        for f in os.listdir(self.base_path):
            yield os.path.join(self.base_path, f)

    def get_destination(self, filename):
        return os.path.join('igrins', '20180527', filename)

    def get_dest_path(self, filename):
        return os.path.join('igrins', '20180527')


if __name__ == "__main__":
    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--dryrun", action="store_true", dest="dryrun", default=False, help="Don't actually do anything")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run in background mode")
    parser.add_option("--force", action="store_true", dest="force", default=False,
                      help="Copy file even if present on disk")
    parser.add_option("--alopeke", action="store_true", dest="alopeke", default=False, help="Copy Alopeke data")
    parser.add_option("--zorro", action="store_true", dest="zorro", default=False, help="Copy Zorro data")
    parser.add_option("--zorro-old", action="store_true", dest="zorroold", default=False,
                      help="Copy Old Zorro data (from /sci/dataflow/zorro-old)")
    parser.add_option("--igrins", action="store_true", dest="igrins", default=False, help="Copy IGRINS data")

    (options, args) = parser.parse_args()

    # Logging level to debug?
    setdebug(options.debug)
    setdemon(options.demon)

    # Annouce startup
    logger.info("*********  copy_from_visiting_instrument.py - starting up at %s" % datetime.datetime.now())

    if using_s3:
        logger.info("This should not be used with S3 storage. Exiting")
        sys.exit(1)
    if options.demon and options.force:
        logger.info("Force not not available when running as daemon")
        sys.exit(2)

    logger.info("Doing Initial visiting instrument directory scan...")

    ingesters = list()
    if options.alopeke:
        ingesters.append(Alopeke())
    if options.zorro:
        ingesters.append(Zorro())
    if options.zorroold:
        ingesters.append(Zorro("/sci/dataflow/zorro-old"))
    if options.igrins:
        ingesters.append(IGRINS())
    if ingesters:
        known_list = set()

        with session_scope() as session:
            done = False
            while not done:
                for ingester in ingesters:
                    # Get initial Alopeke directory listing
                    dir_list = set(ingester.get_files())
                    logger.info("... found %d files", len(dir_list))

                    logger.debug("Instantiating IngestQueueUtil object")
                    iq = IngestQueueUtil(session, logger)

                    # logger.info("Starting looping...")
                    # while True:

                    todo_list = dir_list - known_list
                    logger.info("%d new files to check", len(todo_list))
                    for filename in todo_list:
                        if 'tmp' in filename:
                            logger.info("Ignoring tmp file: %s", filename)
                            continue
                        fullname = filename
                        filename = os.path.split(filename)[1]
                        if check_present(session, filename):
                            logger.debug("%s is already present in database", filename)
                            known_list.add(filename)
                        else:
                            if ingester.copy_over(session, iq, logger, fullname, options.dryrun, options.force):
                                known_list.add(filename)
                if options.demon:
                    logger.debug("Pass complete, sleeping")
                    time.sleep(300)
                else:
                    done = True
                    logger.debug("Exiting")
    else:
        logger.info("No ingesters specified, nothing to copy")
