import sys
import os
import traceback
import datetime
import time
import shutil
from abc import ABC, abstractmethod

from fits_storage.orm import session_scope
from fits_storage.orm.diskfile import DiskFile
from fits_storage.logger import logger, setdebug, setdemon
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
    if filename.endswith(".fits")
        filename2 = "%s.bz2" % filename
        df = session.query(DiskFile).filter(DiskFile.filename==filename2).filter(DiskFile.present==True).first()
        if df:
            return True
    elif filename.endswith(".bz2"):
        filename2 = filename[0:-4]
        df = session.query(DiskFile).filter(DiskFile.filename==filename2).filter(DiskFile.present==True).first()
        if df:
            return True
    return False


def VisitingInstrumentABC(ABC):
    """
    Base class for visiting instrument handling.

    This provides the common framework/structure and the
    implementations handle the peculiarities of each.
    """
    def __init__(self, base_path):
        self.base_path = base_path
    
    def check_filename(self, filename):
        return filename not in ['.bplusvtoc_internal', '.vtoc_internal']

    @abstractmethod
    def get_files(self):
        rase NotImplementedError("subclasses must implement get_files()")

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
        rase NotImplementedError("subclasses must implement get_dest_path()")

    def copy_over(self, session, iq, logger, filename, dryrun):
        src = os.path.join(self.base_path, filename)
        dst_filename = self.get_dest_filename(src)
        dst_path = self.get_dest_path(src)
        dst = os.path.join(storage_root, dst_path, dst_filename)
        dest = os.path.join(storage_root, self.get_destination(filename))
        # If the Destination file already exists, skip it
        if os.access(dst, os.F_OK | os.R_OK):
            logger.info("%s already exists on storage_root - skipping", filename)
            return True
        # If the source path is a directory, skip is
        if os.path.is_dir(src):
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


def Alopeke(VisitingInstrumentABC):
    def __init__(self):
        super().__init__(self, "/net/mkovisdata/home/alopeke/")
        self._foldername_re = re.search('\d8')

    def prep(self):
        if not os.path.exists(os.path.join(storage_root, 'alopeke')):
            os.mkdir(os.path.join(storage_root, 'alopeke'))

    def get_files(self):
        for f in os.listdir(self.base_path):
            fullpath = os.path.join(self.base_path, f)
            if os.path.isdir(fullpath) and self._foldername_re.matches(f):
                for datafile in os.listdir(fullpath):
                    if self._filename_re.matches(datafile):
                        yield os.path.join(f, datafile)

    def get_destination(self, filename):
        return os.path.join('alopeke', filename)


if __name__ == "__main__":
    # Annouce startup
    logger.info("*********  copy_from_visiting_instrument.py - starting up at %s" % datetime.datetime.now())

    if using_s3:
        logger.info("This should not be used with S3 storage. Exiting")
        sys.exit(1)


    logger.info("Doing Initial visiting instrument directory scan...")
    # Get initial DHS directory listing
    ingester = Alopeke()
    dir_list = set(ingester.get_files())
    logger.info("... found %d files", len(dir_list))
    known_list = set()

    with session_scope() as session:
        logger.debug("Instantiating IngestQueueUtil object")
        iq = IngestQueueUtil(session, logger)
        logger.info("Starting looping...")
        while True:
            todo_list = dir_list - known_list
            logger.info("%d new files to check", len(todo_list))
            for filename in todo_list:
                if 'tmp' in filename:
                    logger.info("Ignoring tmp file: %s", filename)
                    continue
                filename = os.path.split(filename)[1]
                if check_present(session, filename):
                    logger.debug("%s is already present in database", filename)
                    known_list.add(filename)
                else:
                    if ingester.copy_over(session, iq, logger, filename, options.dryrun):
                        known_list.add(filename)
            logger.debug("Pass complete, sleeping")
            time.sleep(5)
            logger.debug("Re-scanning")
            dir_list = set(ingester.get_files_list())
