"""
This module contains code for reducing data with DRAGONS
"""

import os
import os.path
import shutil
import bz2

from sqlalchemy.exc import MultipleResultsFound, NoResultFound

from fits_storage.core.orm.diskfile import DiskFile

from fits_storage.config import get_config

if get_config().using_s3:
    from fits_storage.server.aws_s3 import get_helper

# DRAGONS imports
from recipe_system.reduction.coreReduce import Reduce
from gempy.utils.logutils import customize_logger
from recipe_system import cal_service


class Reducer(object):
    """
    This class provides functionality for reducing data using DRAGONS.

    We instantiate this class in service_reduce_queue. We instantiate a new
    instance of this class for each reduction job (ie each entry in the
    reduce queue).

    This class handles creating a working directory where the reduction will
    run, copying in the raw data files, calling DRAGONS, then extracting
    the desired reduced data products from the working directory and adding
    them to the ingest queue.

    """
    def __init__(self, session, logger, rqe, nocleanup=False):
        """
        Instantiate the Reducer class with a session, logger, and
        reducequeueentry instance.

        Pass nocleanup=True to not delete the working directory after
        processing completed.

        Parameters
        ----------
        session - database session
        logger - Fits Storage Logger
        """
        fsc = get_config()
        self.s = session
        self.l = logger
        self.nocleanup = nocleanup
        self.reduced_files = []

        # We pull these configuration values into the local namespace for
        # convenience and to allow poking them for testing
        self.reduce_dir = fsc.reduce_dir
        self.using_s3 = fsc.using_s3

        # We store the reducequeueentry passed for reduction in the class for
        # convenience and to facilitate the seterror convenience function
        self.rqe = rqe

        # Initialize these to None here, they are set as they are used
        self.workingdir = None

    def logrqeerror(self, message, exc_info=False):
        """
        Convenience function to log an error message,
        set the rqe error status, and commit the session
        """
        self.l.error(message, exc_info=exc_info)
        self.rqe.seterror(message)
        self.s.commit()

    def do_reduction(self):
        """
        This is the main action function for the reducer
        """

        self.makeworkingdir()
        self.getrawfiles()
        self.call_reduce()
        self.capture_reduced_files()
        if not self.nocleanup:
            self.cleanup()

    def makeworkingdir(self):
        """
        Make a working directory for this reduction job. We make a subdirectory
        with the name of the reductionqueueentry id inside the reductiondir
        from the fits storage config file.

        This method ensures that the directory exists, and sets
        self.workindir to point to it. It refuses to proceed if the configured
        reduce_dir does not exist in a suitable form.
        """
        if not os.path.isdir(self.reduce_dir):
            self.logrqeerror("Configured reduce_dir does not exist or is not a "
                             "directory: %s" % self.reduce_dir)
            raise OSError

        self.workingdir = os.path.join(self.reduce_dir, str(self.rqe.id))

        if os.path.exists(self.workingdir):
            self.logrqeerror("Reduce Working directory for this reduce queue"
                             "entry ID already exists: %s." % self.workingdir)
            raise OSError

        self.l.info("Creating working directory for this reduction job: %s",
                    self.workingdir)
        try:
            os.mkdir(self.workingdir)
        except Exception:
            self.l.error("Failed to create working directory for this "
                         "reduction job at %s", self.workingdir, exc_info=True)


    def getrawfiles(self):
        """
        Copy the raw files into the working directory, and uncompress them if
        required.

        Returns True on success, False on failure.
        """

        for filename in self.rqe.filenames:
            possible_filenames = [filename, filename+'.bz2']
            query = self.s.query(DiskFile).filter(DiskFile.present == True)\
                .filter(DiskFile.filename.in_(possible_filenames))
            try:
                df = query.one()
            except NoResultFound:
                self.logrqeerror('Cannot find diskfile for filename %s'
                                 % filename)
                return False
            except MultipleResultsFound:
                self.logrqeerror('Multiple results for diskfile with filename'
                                 '%s' % filename)
                return False

            # At this point, df should be a valid DiskFile object
            if self.using_s3:
                s3helper = get_helper()
                # Fetch from S3. Note that if it's S3 it's almost certainly
                # compressed too. Use a with fetch_temporary from aws_s3 so that
                # we don't leave it behind in s3_staging after we decompress it?
                self.l.error("S3 not implemented yet is reducer.py")
                return False

            # Copy the file into the working directory, decompressing it if
            # appropriate to ensure that AstroData doesn't end up doing that
            # multiple times and to facilitate memory mapping
            outfile = os.path.join(df.filename.removesuffix('.bz2'),
                                   self.workingdir, filename)
            if df.compressed:
                chunksize = 1000000  # 1MB
                self.l.debug(f"Decompressing {df.fullpath} into {outfile}")
                # We could verify the data md5 too while we do this. Size is
                # a good quick sanity check for now though.
                numbytes = 0
                with bz2.open(df.fullpath, "rb") as infile:
                    with open(outfile, "wb") as outfile:
                        while True:
                            chunk = infile.read(chunksize)
                            if not chunk:
                                break
                            numbytes += len(chunk)
                            outfile.write(chunk)
                if numbytes != df.data_size:
                    self.l.warning("Did not get correct number of bytes "
                                   "when decompressing %s", df.fullpath)
            else:
                # Simple file copy
                shutil.copyfile(df.fullpath, outfile)

        return True

    def capture_reduced_files(self):
        self.l.info("Capture Reduced Files not implemented yet")
        # Consideration of how we will use this operationally. If we are
        # running on a "standalone" fits storage instance, the simplest thing
        # would be to copy the files to storage_root and ingest them, having
        # configured export_destinations to push these to the archive and tape
        # server as appropriate. However, we have to have direct access to the
        # database where the reducequeue lives, which would nominally be the
        # archive database, so we're not standalone in the sense that we don't
        # have an independent database. In that context, what we want to do is
        # upload the files to S3, and then ingest them, ie upload_ingest.

        return False

    def cleanup(self):
        """
        Clean up all residue from this reduce instance. Do not call this until
        you have copied anything you want to keep from the working directory.
        """

        if self.workingdir is not None:
            self.l.debug("Cleanup deleting reduction working directory %s",
                         self.workingdir)
            shutil.rmtree(self.workingdir, ignore_errors=True)
            self.workingdir = None
        else:
            self.l.debug("No reduction working directly to clean up")

    def call_reduce(self):
        """
        Invoke the DRAGONS Reduce() class.
        """
        # See https://dragons.readthedocs.io/projects/recipe-system-users-manual/en/v3.2.3/appendices/full_api_example.html#api-example

        # Add the DRAGONS customizations (ie additional log levels) to logger
        customize_logger(self.l)

        # Need to figure out how we want to handle reduced calibrations, and
        # how to configure the calmgr appropriately.
        configstring = """
        [calibs]
        databases = https://archive.gemini.edu get
        """
        dragons_config = cal_service.globalConf
        dragons_config.read_string(configstring)

        self.l.debug("DRAGONS calibs Configuration:")
        for key in dragons_config['calibs']:
            self.l.debug(f"{key} : {dragons_config['calibs'][key]}")

        # Call Reduce()
        reduce = Reduce()

        # Tell Reduce() what files to reduce
        reduce.files.extend(self.rqe.filenames)

        # Tell Reduce() not to upload calibrations (for now)
        reduce.upload = None

        # chdir into the working directory for DRAGONS. Store the current
        # working dir so we can go back after
        pwd = os.getcwd()

        try:
            os.chdir(self.workingdir)
            self.l.info("Calling DRAGONS Reduce.runr() in directory %s",
                        self.workingdir)
            reduce.runr()
        except Exception:
            self.logrqeerror("Exception from DRAGONS Reduce()", exc_info=True)
        finally:
            os.chdir(pwd)

        self.l.info("DRAGONS Reduce.runr() appeared to complete sucessfully")
        self.reduced_files = reduce.output_filenames
        self.l.info("Output files are: %s", reduce.output_filenames)

