import signal

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

"""
Script to copy files from the various Visting Instrument staging areas into Dataflow.
"""


# Utility functions
def check_present(session, filename):
    """
    Check if the given filename is present in the database.

    This method checks the file against the `fits_storage.orm.DiskFile` table to
    see if it already exists and is marked present.  It differs slightly from the
    DHS version of this logic in that it will look for both a `.bz2` and non-`.bz2`
    variant of the file.

    Parameters
    ----------

    session : `sqlalchemy.orm.session.Session`
        SQLAlchemy session to check against
    filename : str
        Name of the file to look for

    Returns
    -------
        True if a record exists in `fits_storage.orm.DiskFile` for this filename with `present` set to True
    """
    otherfilename = filename
    if otherfilename.endswith('.bz2'):
        otherfilename = otherfilename[:-4]
    else:
        otherfilename = "%.bz2" % otherfilename
    df = session.query(DiskFile).filter(DiskFile.filename==filename).filter(DiskFile.canonical==True).first()
    if df:
        return True
    df = session.query(DiskFile).filter(DiskFile.filename==otherfilename).filter(DiskFile.canonical==True).first()
    if df:
        return True
    return False


class VisitingInstrumentABC(ABC):
    """
    Base class for visiting instrument handling.

    This provides the common framework/structure and the
    implementations handle the peculiarities of each.
    """
    def __init__(self, base_path, apply_fixes, storage_root=storage_root):
        self.base_path = base_path
        self.apply_fixes = apply_fixes
        self.storage_root = storage_root
    
    def check_filename(self, filename):
        """
        Check if the given filename is valid.

        This method checks the filename to see if it is
        one we know we should ignore.

        Parameters
        ----------

        filename : str
            Name of the file to look for

        Returns
        -------
            True if a filename is one we want to ingest, False if it's a known ignoreable file
        """
        return filename not in ['.bplusvtoc_internal', '.vtoc_internal']

    @abstractmethod
    def get_files(self):
        """
        Abstract method for getting all the files to be copied.

        This method should be implemented to return a list of all
        the files to be ingested for that visiting instrument.

        Returns
        -------
            list of str filenames to ingest
        """
        raise NotImplementedError("subclasses must implement get_files()")

    def prep(self):
        """
        Perform any necessary pre-work.

        This should be implemented by sub-classes if there are any steps they
        want to take before ingesting files.  This could be, for example, creating
        required destination folder in dataflow for this visiting instrument.
        """
        return

    @abstractmethod
    def get_dest_path(self, filename):
        """
        Abstract method to get the destination path for a given filename.

        This method builds the full path to the destination on disk for the
        visiting instrument datafile.

        Parameters
        ----------

        filename : str
            Name of the file to be mapped to a destination folder

        Returns
        -------

        str : full path of the folder to copy the file to
        """
        raise NotImplementedError("subclasses must implement get_dest_path()")

    def get_dest_filename(self, filename):
        """
        Get the filename for the file in the destination.

        This helper method pulls out the base name of the file, but
        could be overridden if there's a need to alter the filename.

        Parameters
        ----------

        filename : str
            Name of the file input from the staging area


        Returns
        -------
        str : name of the file to write to the DataFlow area
        """
        return os.path.basename(filename)

    def target_found(self, dst):
        """
        Check if the given file is found.

        This does a number of checks for the given file.
        It checks if the file exists and is readable.  It
        will look for both a `.bz2` and non-`.bz2` version
        of the file.

        Parameters
        ----------

        dst : str
            File to check for existance/access

        Returns
        -------
        bool : True if file exists, is a file, and is readable
        """
        if os.access(dst, os.F_OK | os.R_OK):
            return True
        if dst.endswith('.bz2'):
            if os.access(dst[:-4], os.F_OK | os.R_OK):
                return True
        else:
            if os.access('%s.bz2' % dst, os.F_OK | os.R_OK):
                return True

    def copy_over(self, session, iq, logger, filename, dryrun, force):
        """
        Copy a file over from the visiting instrument staging area to DataFlow.

        This method copies a specified file, by name, from the visiting
        instrument staging area over to the appropriate location in dataflow.
        It relies on the various abstract methods to tell it where to find
        the file in staging and where it should put it in dataflow.

        Files that have been written to within the last 5 seconds are not
        copied and will come back in a later pass.

        Parameters
        ----------
        session : sqlalchemy.orm.session.Session
            SQLAlchemy session, unused
        iq : fits_storage.orm.ingestqueue.IngestQueue
            Ingest queue to add file to after copy
        logger : logging.logger
            Logger for log messages
        filename : str
            Name of file to copy
        dryrun : bool
            If True, don't do any actual changes, just check what would be done
        force : bool
            If True, copy and ingest file, even if it already has a `fits_storage.orm.diskfile.DiskFile` record

        Returns
        -------
        bool : True if file copied or ignored, False if there was an error or the file is too new to copy
        """
        src = os.path.join(self.base_path, filename)
        dst_filename = self.get_dest_filename(src)
        logger.debug("Calcuating dst_path from src=%s" % src)
        dst_path = self.get_dest_path(filename)
        logger.debug("Creating dst from %s %s %s" % (self.storage_root, dst_path, dst_filename))
        dst = os.path.join(self.storage_root, dst_path, dst_filename)
        # If the Destination file already exists, skip it
        if not force and self.target_found(dst):
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
                logger.info("Copying %s to %s", filename, self.storage_root)
                # We can't use shutil.copy, because it preserves mode of the
                # source file, making the umask totally useless. Under the hood,
                # copy is just a shutil.copyfile + shutil.copymode. We'll
                # use copyfile instead.
                if not os.path.exists(os.path.join(self.storage_root, dst_path)):
                    os.mkdir(os.path.join(self.storage_root, dst_path))
                if self.apply_fixes:
                    fix_and_copy(os.path.dirname(src), os.path.join(self.storage_root, dst_path), dst_filename,
                                 True, _mailfrom, _mailto)
                else:
                    shutil.copyfile(src, dst)
                logger.info("Adding %s to IngestQueue", filename)
                
                iq.add_to_queue(dst_filename, dst_path, force=False, force_md5=False, after=None)
        except:
            logger.error("Problem copying %s to %s", src, self.storage_root)
            logger.error("Exception: %s : %s... %s", sys.exc_info()[0], sys.exc_info()[1],
                                                    traceback.format_tb(sys.exc_info()[2]))
            return False
        # Add it to the ingest queue here
        return True


class AlopekeZorroABC(VisitingInstrumentABC):
    """
    Base class for Alopeke and Zorro handling.

    Since Alopeke and Zorro are so similar, we have another
    intermediate abstract class for handling them.  There
    are still some peculiarities we want broken out into
    the final implementations later.
    """
    def __init__(self, instr, path, apply_fixes, storage_root=storage_root):
        """
        Initialize the class with the given settings.

        The subclass can use these parameters to help tune the common
        functionality for whichever instrument we are dealing with.

        Parameters
        ----------
        instr : str
            instrument being implemented, `zorro` or `alopeke`
        path : str
            Path for the instrument data in staging
        apply_fixes : bool
            If True, we want to apply fixes to the data
        storage_root : str
            Path to dataflow
        """
        super().__init__(path, apply_fixes, storage_root=storage_root)
        self._instrument = instr

    def prep(self):
        """
        Prepare for Alopeke/Zorro data.

        This method prepares dataflow by creating the `alopeke` or `zorro` folder
        in dataflow, if it is not present.
        """
        if not os.path.exists(os.path.join(self.storage_root, self._instrument)):
            os.mkdir(os.path.join(self.storage_root, self._instrument))

    def get_files(self):
        """
        Get the files to copy.

        Returns
        -------
            list of str : paths relative to the staging area for the files to copy
        """
        for f in os.listdir(self.base_path):
            fullpath = os.path.join(self.base_path, f)
            if os.path.isdir(fullpath) and re.search(r'^\d{8}$', f):
                for datafile in os.listdir(fullpath):
                    matched = False
                    for rex in self._filename_res:
                        if rex.search(datafile):
                            matched = True
                    if matched:
                        yield os.path.join(f, datafile)

    def get_dest_path(self, filename):
        """
        Get the path within the instrument dataflow area for this file.

        Returns
        -------
            str : For these, the path is a YYYYMMDD style datestring and it's inferred from the staging folder
        """
        rel_path = os.path.split(filename)[0]
        return os.path.join(self._instrument, rel_path)


class Alopeke(AlopekeZorroABC):
    """
    Alopeke implementation for copying visiting instrument data.
    """
    def __init__(self):
        """
        Create Alopeke visiting instrument data copier
        """
        super().__init__('alopeke', "/net/mkovisdata/home/alopeke/", True)
        self._filename_res = [re.compile(r'N202\d{5}A\d{4}[br].fits.bz2'), ]


class Zorro(AlopekeZorroABC):
    """
    Zorro implementation for copying visiting instrument data.
    """
    def __init__(self, base_path="/net/cpostonfs-nv1/tier2/ins/sto/zorro/"):
        """
        Create Zorro visiting instrument data copier
        """
        super().__init__('zorro', base_path, True)
        self._filename_res = [re.compile(r'S202\d{5}Z\d{4}[br].fits.bz2'), ]


class IGRINS(VisitingInstrumentABC):
    """
    IGRINS implementation for copying visiting instrument data.
    """
    def __init__(self, base_path="/sci/dataflow/igrins/igrins-rawfiles/", storage_root=storage_root):
        """
        Create IGRINS visiting instrument data copier
        """
        super().__init__(base_path, True, storage_root=storage_root)
        self._date_re = re.compile(r'[A-Z]{4}_(\d{8})_\d{4}.*\.fits')

    def prep(self):
        """
        Prepare for IGRINS ingest by ensuring we have an `igrins` folder in dataflow
        """
        if not os.path.exists(os.path.join(self.storage_root, 'igrins')):
            os.mkdir(os.path.join(self.storage_root, 'igrins'))

    def get_files(self):
        """
        Get the files within the staging area to ingest.

        Returns
        -------
        list of str : list of files in the top level to be copied
        """
        for f in os.listdir(self.base_path):
            if self._date_re.match(f):
                yield f

    def get_dest_path(self, filename):
        """
        Get the location within dataflow to copy the IGRINS data to.

        Returns
        -------
            str : path within dataflow, something like `igrins/20200102`
        """
        result = self._date_re.match(os.path.basename(filename))
        ymd = result.group(1)
        return os.path.join('igrins', ymd)


_mailfrom = "fitsdata@gemini.edu"
_mailto = None


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
    parser.add_option("--emailto", action="store", dest="emailto", default=None, help="Where to send error emails")

    (options, args) = parser.parse_args()

    # Logging level to debug?
    setdebug(options.debug)
    setdemon(options.demon)

    if options.emailto:
        _mailto = options.emailto

    # Annouce startup
    logger.info("*********  copy_from_visiting_instrument.py - starting up at %s" % datetime.datetime.now())

    # Need to set up the global loop variable before we define the signal handlers
    # This is the loop forever variable later, allowing us to stop cleanly via kill
    global loop
    loop = True

    # Define signal handlers. This allows us to bail out neatly if we get a signal
    def handler(signum, frame):
        logger.error("Received signal: %d. Crashing out. ", signum)
        raise KeyboardInterrupt('Signal', signum)

    def nicehandler(signum, frame):
        logger.error("Received signal: %d. Attempting to stop nicely ", signum)
        global loop
        loop = False

    # Set handlers for the signals we want to handle
    # Cannot trap SIGKILL or SIGSTOP, all others are fair game
    signal.signal(signal.SIGHUP, nicehandler)
    signal.signal(signal.SIGINT, nicehandler)
    signal.signal(signal.SIGQUIT, nicehandler)
    signal.signal(signal.SIGILL, handler)
    signal.signal(signal.SIGABRT, handler)
    signal.signal(signal.SIGFPE, handler)
    signal.signal(signal.SIGSEGV, handler)
    signal.signal(signal.SIGPIPE, handler)
    signal.signal(signal.SIGTERM, nicehandler)

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
                        if check_present(session, filename) and not options.force:
                            logger.debug("%s is already present in database", filename)
                            known_list.add(filename)
                        else:
                            if ingester.copy_over(session, iq, logger, fullname, options.dryrun, options.force):
                                known_list.add(filename)
                if options.demon and loop:
                    logger.debug("Pass complete, sleeping")
                    time.sleep(300)
                else:
                    done = True
                    logger.debug("Exiting")
    else:
        logger.info("No ingesters specified, nothing to copy")
