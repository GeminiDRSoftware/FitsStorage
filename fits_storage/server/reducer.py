"""
This module contains code for reducing data with DRAGONS
"""

import os
import os.path
import shutil

from sqlalchemy.exc import MultipleResultsFound, NoResultFound

from fits_storage.core.orm.diskfile import DiskFile

from fits_storage.config import get_config

if get_config().using_s3:
    from fits_storage.server.aws_s3 import get_helper

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
    def __init__(self, session, logger, rqe):
        """
        Instantiate the Reducer class with a session, logger, and the
        reducequeueentry to reduce.

        Parameters
        ----------
        session - database session
        logger - Fits Storage Logger
        """
        fsc = get_config()
        self.s = session
        self.l = logger

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

    def cleanup(self):
        """
        Clean up all residue from this reduce instance. Do not call this until
        you have copied anything you want to keep from the working directory.
        """

        if self.workingdir is not None:
            shutil.rmtree(self.workingdir, ignore_errors=True)
            self.workingdir = None

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

        os.mkdir(self.workingdir)

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
                # Fetch from S3
            else:
                if df.compressed:
                    #
