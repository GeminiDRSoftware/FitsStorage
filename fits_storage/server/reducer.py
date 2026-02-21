"""
This module contains code for reducing data with DRAGONS
"""
import ast
import io
import logging
import os
import os.path
import shutil
import bz2
import json
import http
import requests
import gc
import psutil

from astropy.io import fits

import astrodata
import gemini_instruments

from sqlalchemy.exc import MultipleResultsFound, NoResultFound

from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header
from fits_storage.gemini_metadata_utils import gemini_processing_modes

from fits_storage.queues.queue.fileopsqueue import FileopsQueue, FileOpsRequest
from fits_storage.queues.orm.reducequeentry import ReduceQueueEntry

from fits_storage.server.orm.monitoring import Monitoring
from fits_storage.server.orm.processinglog import ProcessingLog
from fits_storage.server.monitoring import get_recipe_keywords

from fits_storage.core.hashes import md5sum
from fits_storage.server.bz2stream import StreamBz2Compressor

# DRAGONS imports
from recipe_system.reduction.coreReduce import Reduce
from gempy.utils.logutils import customize_logger
from recipe_system import cal_service

from fits_storage.config import get_config
if get_config().using_s3:
    from fits_storage.server.aws_s3 import Boto3Helper

class ReducerMemoryLeak(Exception):
    pass

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
        self.fsc = get_config()
        self.s = session
        self.l = logger
        self.nocleanup = nocleanup
        self.reduced_files = []

        # Initialize this to None here. We *may* be able to populate this later,
        # but generally only rigorously for single file reductions. For groups,
        # we could set it to the first file, which should be somewhat
        # representative, but that could be misleading in some cases.
        self.header_id = None

        # We pull these configuration values into the local namespace for
        # convenience and to allow poking them for testing
        self.reduce_dir = self.fsc.reduce_dir
        self.reduce_calcache_dir = self.fsc.reduce_calcache_dir
        self.reduce_calcache_gbs = self.fsc.reduce_calcache_gbs
        self.using_s3 = self.fsc.using_s3
        self.upload_url = self.fsc.reducer_upload_url

        # We store the reducequeueentry passed for reduction in the class for
        # convenience and to facilitate the seterror convenience function
        self.rqe = rqe

        # Initialize these to None here, they are set as they are used
        self.workingdir = None

        # When we call reduce with no recipe, the recipe becomes '_default', but
        # reduce.runr() returns the actual recipe name used. We record that and
        # reference it in eg set_reduction_metadata and capture_monitoring
        self.actual_recipe = None

        # If we are uploading reduced files to an upload_url, make the
        # requests session here.
        if self.upload_url:
            self.rs = requests.Session()
            requests.utils.add_dict_to_cookiejar(
                self.rs.cookies,
                {'gemini_fits_upload_auth': self.fsc.export_auth_cookie})


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
        self.validate()

        # There's a subtle gotcha in that with sqlalchemy, almost
        # any access to the data values of an ORM instance will
        # start a transaction and until that transaction ends
        # (ie COMMIT;s) we cannot get an ACCESS EXCLUSIVE lock to
        # pop another queue entruy (in any process). So in all the
        # reducequeue and reducer code, we need to be diligent about
        # doing a session.commit() after we are done accessing
        # the reducequeue instance, even if we didn't modify it.


        if not self.rqe.failed:
            self.s.commit()  # Ensure transaction on rqe is closed
            self.makeworkingdir()

        if not self.rqe.failed:
            self.s.commit()  # Ensure transaction on rqe is closed
            self.getrawfiles()

        if not self.rqe.failed:
            self.s.commit()  # Ensure transaction on rqe is closed
            self.call_reduce()

        if not self.rqe.failed:
            self.s.commit()  # Ensure transaction on rqe is closed
            self.set_reduction_metadata()

        if not self.rqe.failed:
            self.s.commit()  # Ensure transaction on rqe is closed
            if self.rqe.capture_files:
                self.s.commit()  # Ensure transaction on rqe is closed
                self.capture_reduced_files()
            else:
                self.l.info("Not capturing Output files from this processing "
                            "run")

        if not self.rqe.failed:
            self.s.commit()  # Ensure transaction on rqe is closed
            if self.rqe.capture_monitoring:
                self.s.commit()  # Ensure transaction on rqe is closed
                self.capture_monitoring()
            else:
                self.l.info("Not capturing monitoring values from this "
                            "processing run")

        if self.nocleanup or (self.fsc.fits_system_status == 'development'
                              and self.rqe.failed):
            self.s.commit()  # Ensure transaction on rqe is closed
            self.l.warning("Not cleaning up working directory")
        else:
            self.cleanup()

        if self.rqe.failed:
            self.s.commit()  # Ensure transaction on rqe is closed
            return False

        # Yay. If we got here, we successfully reduced the data.
        self.l.debug("Deleting competed rqe id %d", self.rqe.id)
        self.s.delete(self.rqe)
        self.s.commit()

        # Delete any equivlent rqe entries that are marked as failed
        failed_rqes = self.s.query(ReduceQueueEntry) \
            .filter(ReduceQueueEntry.fail_dt != self.rqe.fail_dt_false) \
            .filter(ReduceQueueEntry.filename == self.rqe.filename) \
            .filter(ReduceQueueEntry.filenames == self.rqe.filenames) \
            .filter(ReduceQueueEntry.intent == self.rqe.intent) \
            .filter(ReduceQueueEntry.initiatedby == self.rqe.initiatedby) \
            .filter(ReduceQueueEntry.tag == self.rqe.tag)

        for failed_rqe in failed_rqes:
            self.l.info("Deleting failed reducequeue entry %d having "
                        "successfully processed equivalent request" %
                        failed_rqe.id)
            self.s.delete(failed_rqe)
        self.s.commit()

        # As of 2025-06-17 DRAGONS Reduce() appears to create cyclic object
        # references, or otherwise causes the effects of a memory leak.
        # Calling gc.collect() seems to mitigate this.
        # We check our memory footprint after calling for garbage
        # collection, and raise ReducerMemoryLeak if so, which is picked up by
        # service_reduce_queue to cause an exit. This needs to happen right at
        # the end of do_reduction(), not in call_dragons() so that we clean-up
        # properly from the reduction we just did.

        gc.collect()
        rss = psutil.Process().memory_info().rss
        rss /= 1E6
        # 400 (MB) seems typical on linux, python 3.12, once things
        # are underway.
        if rss > 600:
            self.l.error(f'Reducer memory footprint excessive: {rss} MB.')
            self.l.error('Raising ReducerMemoryLeak to trigger restart')
            raise ReducerMemoryLeak

    def validate(self):
        """
        Validate the request looks sensible and has the required metadata
        before starting work on it.

        Set self.rqe.failed via logrqeerror if not.

        """
        if len(self.rqe.filenames) == 0:
            self.logrqeerror("Reducer Validation failed: No files to reduce")

        if self.rqe.intent not in gemini_processing_modes:
            self.logrqeerror("Reducer Validation failed: Invalid "
                             f"Processing Intent: {self.rqe.intent}")

        if not self.rqe.initiatedby:
            self.logrqeerror("Reducer Validation failed: No Processing "
                             "Initiated By value")
        if not self.rqe.tag:
            self.logrqeerror("Reducer Validation failed: No Processing Tag "
                             "value")

        self.s.commit()  # Ensure transaction on rqe is closed

        # Check upload_staging_dir is configured and exists as we need it to
        # capture the reduced products.
        if not (self.fsc.upload_staging_dir and
                os.path.exists(self.fsc.upload_staging_dir) and
                os.path.isdir(self.fsc.upload_staging_dir)):
            self.logrqeerror(f"Upload Staging Directory "
                             f"{self.fsc.upload_staging_dir} not valid")


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
            self.logrqeerror(f"Configured reduce_dir does not exist or is not a "
                             f"directory: {self.reduce_dir}")
            return

        self.workingdir = os.path.join(self.reduce_dir, str(self.rqe.id))
        self.s.commit()  # Ensure transaction on rqe is closed

        if os.path.exists(self.workingdir):
            self.logrqeerror(f"Reduce Working directory for this reduce queue"
                             f"entry ID already exists: {self.workingdir}")
            return

        self.l.info("Creating working directory for this reduction job: %s",
                    self.workingdir)
        try:
            os.mkdir(self.workingdir)
        except Exception:
            self.logrqeerror(f"Failed to create working directory for this "
                             f"reduction job at {self.workingdir}",
                             exc_info=True)
            raise


    def getrawfiles(self):
        """
        Copy the raw files into the working directory, and uncompress them if
        required.

        Set self.rqe.failed via logrqeerror if fails.
        """
        filenames = self.rqe.filenames
        self.s.commit()  # Ensure transaction on rqe is closed

        for filename in filenames:
            possible_filenames = [filename, filename+'.bz2']
            query = self.s.query(DiskFile).filter(DiskFile.present == True)\
                .filter(DiskFile.filename.in_(possible_filenames))
            try:
                df = query.one()
            except NoResultFound:
                self.logrqeerror('Cannot find diskfile for filename %s'
                                 % filename)
                return
            except MultipleResultsFound:
                self.logrqeerror('Multiple results for diskfile with filename'
                                 '%s' % filename)
                return False

            # Grab the (or a representative, ie first) header_id at this point.
            # This is used in capture_monitoring()
            if self.header_id is None:
                try:
                    header = self.s.query(Header)\
                        .filter(Header.diskfile_id == df.id).one()
                    self.header_id = header.id
                except (NoResultFound, MultipleResultsFound):
                    pass

            # At this point, df should be a valid DiskFile object
            if self.using_s3:
                s3helper = Boto3Helper()
                # Fetch from S3 into the working dir
                keyname = f"{df.path}/{df.filename}" if df.path else df.filename
                self.l.info(f"Fetching {keyname} from S3 to decompress")
                working_filename = df.filename.removesuffix('.bz2')
                outfile = os.path.join(self.workingdir, working_filename)
                self.l.info(f"Fetching from S3 into {outfile}")
                if df.compressed:
                    s3flo = s3helper.get_flo(keyname)
                    with bz2.BZ2File(s3flo) as infile:
                        chunksize = 1000000  # 1MB
                        numbytes = 0
                        with open(outfile, "wb") as fp:
                            while True:
                                chunk = infile.read(chunksize)
                                if not chunk:
                                    break
                                numbytes += len(chunk)
                                fp.write(chunk)
                    if numbytes != df.data_size:
                        self.l.warning("Did not get correct number of bytes "
                                       "when decompressing %s", df.fullpath)
                    s3flo.close()
                else:
                    # Uncompressed file on S3
                    s3helper.fetch_to_storageroot(keyname, outfile)
            else:
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
                    try:
                        shutil.copyfile(df.fullpath, outfile)
                    except Exception:
                        self.logrqeerror(f"Failed to copy raw data file from "
                                         f"{df.fullpath} to {outfile}",
                                         exc_info=True)
                        return

    def set_reduction_metadata(self):
        """
        Ensure that the captured files have the appropriate reduction metadata

        Set self.rqe.failed via logrqeerror if fails.
        """
        # PROCSOFT [DRAGONS | Free form string]
        # PROCSVER [version string]
        # PROCMODE [Science-Quality | Quick-Look]
        # should all have been added by DRAGONS.
        #
        # Processing Intent - PROCITNT [Science-Quality | Quick-Look]
        # Processing initiated by - PROCINBY [Free form string,
        #           with reserved values such as GOA, Gemini-SOS, e.t.c.]
        # Processing Level - PROCLEVL [Integer. Blank for undefined]
        # Processing Tag - PROCTAG [Gemini assigned string]

        dragons_headers = ('PROCSOFT', 'PROCSVER', 'PROCMODE')
        for filename in self.reduced_files:
            fullpath = os.path.join(self.workingdir, filename)
            with fits.open(fullpath, mode='update',
                           do_not_scale_image_data=True) as hdul:
                hdr = hdul[0].header

                # Check that the dragons provided headers are present and correct
                for kw in dragons_headers:
                    if kw not in hdr:
                        self.logrqeerror(f"Reduced File {filename} is missing {kw} "
                                         f"header! Aborting.")
                        return
                    if not hdr[kw]:
                        self.logrqeerror(f"Reduced File {filename} has null {kw} "
                                         f"header! Aborting.")
                        return
                if hdr['PROCSOFT'] != 'DRAGONS':
                    self.logrqeerror(f"Reduced File {filename} was apparently not "
                                     f"reduced with DRAGONS, but {hdr['PROCSOFT']}. "
                                     f"Aborting.")
                    return

                # Check / expand PROCMODE header
                if hdr['PROCMODE'] == 'sq':
                    hdr['PROCMODE'] = 'Science-Quality'
                elif hdr['PROCMODE'] == 'ql':
                    hdr['PROCMODE'] = 'Quick-Look'
                elif hdr['PROCMODE'] == 'qa':
                    hdr['PROCMODE'] = 'Quality-Assessment'

                if hdr['PROCMODE'] not in gemini_processing_modes:
                    self.logrqeerror(f"Reduced File {filename} has invalid PROCMODE "
                                     f"value: {hdr['PROCMODE']}. Aborting!")
                    return

                # Add the values from the rqe
                hdr['PROCITNT'] = self.rqe.intent
                hdr['PROCINBY'] = self.rqe.initiatedby
                hdr['PROCTAG'] = self.rqe.tag
                self.s.commit()  # Ensure transaction on rqe is closed

                # And the actual recipe name
                hdr['DRECIPE'] = self.actual_recipe

                for kw in ['PROCSOFT', 'PROCSVER', 'PROCMODE', 'PROCITNT',
                           'PROCINBY', 'PROCTAG', 'DRECIPE']:
                    if kw in hdr:
                        self.l.debug(f"{filename}: {kw} = {hdr[kw]}")
                    else:
                        self.l.debug(f"{filename}: {kw} MISSING")

    def capture_reduced_files(self):
        # Metadata should be already set.
        # Set self.rqe.failed via logrqeerror if fails.

        if self.upload_url:
            # Compress and Post the uploaded files to self.upload_url
            for filename in self.reduced_files:
                if self.export_reduced_file(filename):
                    return
        else:
            # We ingest the files locally  - ie copy the files to upload_staging
            # and add a fileops queue entry to upload_ingest them. This doesn't
            # work for an archive / distriubuted environment as the fileops
            # queue entry could get serviced by any host

            foq = FileopsQueue(self.s, logger=self.l)
            for filename in self.reduced_files:

                # Copy to upload staging, put in tag directory there
                src = os.path.join(self.workingdir, filename)
                dstpath = os.path.join(self.fsc.upload_staging_dir,
                                       self.rqe.tag)
                self.s.commit()  # Ensure transaction on rqe is closed
                # filename at this point (and correctly in the src) can have
                # directories, eg calibrations/processed_cheese/blah.fits.
                # Need to strip those off for the destination.
                dst = os.path.join(dstpath, os.path.basename(filename))
                self.l.info(f"Copying {src} to {dst}")
                try:
                    os.makedirs(dstpath, exist_ok=True)
                    shutil.copyfile(src, dst)
                except Exception:
                    self.logrqeerror(f"Exception copying {src} to {dst}",
                                     exc_info=True)
                    return

                # Add a fileops ingest_upload queue entry
                self.l.info(f"Adding fileops ingest_upload request for {filename}")
                fo_req = FileOpsRequest(request="ingest_upload",
                                        args={"filename": os.path.basename(filename),
                                              "processed_cal": False,
                                              "path": self.rqe.tag,
                                              "fileuploadlog_id": None,
                                              "batch": self.rqe.batch})

                foq.add(fo_req, filename=os.path.basename(filename),
                        response_required=False, batch=self.rqe.batch)
                self.s.commit()  # Ensure transaction on rqe is closed



    def export_reduced_file(self, filename):
        # Compress and export a file to a remote server.
        # logrqerror and return True on error

        # Get a file-like-object for the data to export
        # Note that we always export bz2 compressed data.
        fpfn = os.path.join(self.workingdir, filename)
        with open(fpfn, mode='rb') as f:
            if filename.endswith('.bz2'):
                destination_filename = filename
                flo = f
            else:
                destination_filename = filename + '.bz2'
                flo = StreamBz2Compressor(f)

            # destination filename should never have leading calibrations/...
            destination_filename = os.path.basename(destination_filename)
            # Construct upload URL.
            dst = os.path.join(self.rqe.tag, destination_filename)
            self.s.commit()  # Ensure transaction on rqe is closed

            url = f"{self.upload_url}/{dst}"
            if self.rqe.batch:
                url += f'?batch={self.rqe.batch}'
                self.s.commit()  # Ensure transaction on rqe is closed

            self.l.info(f"Transferring file {filename} to {url}")

            try:
                req = self.rs.post(url, data=flo, timeout=10)
            except requests.Timeout:
                self.logrqeerror(f"Timeout posting to: {url}",
                                 exc_info=True)
                return True
            except requests.ConnectionError:
                self.logrqeerror(f"ConnectionError posting to: {url}",
                                 exc_info=True)
                return True
            except requests.RequestException:
                self.logrqeerror(f"RequestException posting to: {url}",
                                 exc_info=True)
                return True

            self.l.debug("Got http status %s and response %s",
                         req.status_code, req.text)
            if req.status_code != http.HTTPStatus.OK:
                self.logrqeerror(
                    f"Bad HTTP status: {req.status_code} from "
                    f"upload post to url: {url}")
                return True

            # The response should be a short json document
            if flo is f:
                # Need to provide the extra properties that our bz2streamer
                # provides that are used in the verification.
                flo.bytes_output = os.path.getsize(fpfn)
                flo.md5sum_output = md5sum(fpfn)
            try:
                verification = json.loads(req.text)[0]
                if verification['filename'] != destination_filename:
                    et = "Transfer Verification Filename mismatch: " \
                         f"{verification['filename']} vs {destination_filename}"
                    self.logrqeerror(et)
                    return True
                if verification['size'] != flo.bytes_output:
                    et = "Transfer Verification size mismatch: " \
                            f"{verification['size']} vs {flo.bytes_output}"
                    self.logrqeerror(et)
                    return True
                if verification['md5'] != flo.md5sum_output:
                    et = "Transfer Verification md5 mismatch: " \
                         f"{verification['md5']} vs {flo.md5sum_output}"
                    self.logrqeerror(et)
                    return True

            except (TypeError, ValueError, KeyError):
                error_text = "Exception during transfer verification"
                self.logrqeerror(error_text, exc_info=True)
                return True

            self.l.debug("Transfer Verification succeeded")
        return False

    def capture_monitoring(self):
        """
        Capture monitoring values from the reduced data files
        """
        for filename in self.reduced_files:
            self.l.debug(f"Capturing Monitoring values from {filename}")

            # rqe.recipe may be None or '_default_. self.actual_recipe is set
            # from the actual recipe name that reduce.runr() reported using.
            self.l.debug(f"Actual Recipe name was: {self.actual_recipe}")

            try:
                fpfn = os.path.join(self.workingdir, filename)
                ad = astrodata.open(fpfn)
                keywords = get_recipe_keywords(self.actual_recipe, filename)
                if len(keywords) == 0:
                    self.l.warning(f"No keywords to capture for {filename}")
                for keyword in keywords:
                    # Is the value in the PHU?
                    value = ad.phu.get(keyword)
                    if value is not None:
                        mon = Monitoring(ad)
                        mon.recipe = self.actual_recipe
                        mon.keyword = keyword
                        mon.header_id = self.header_id
                        mon.label = 'PHU'
                        mon.set_value(value)
                        self.s.add(mon)
                        self.s.commit()
                    else:
                        # Don't go through the slices if we got it from the PHU
                        for slice in ad:
                            # Is the value in the HDR?
                            value = slice.hdr.get(keyword)
                            if value is not None:
                                mon = Monitoring(slice)
                                mon.recipe = self.actual_recipe
                                mon.keyword = keyword
                                mon.header_id = self.header_id
                                mon.label = slice.amp_read_area()
                                mon.set_value(value)
                                self.s.add(mon)
                                self.s.commit()
                # Quote-unquote close the astrodata instance
                ad = None
            except Exception:
                self.l.warning("Exception capturing monitoring data",
                               exc_info=True)

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
        Add an entry to ProcessingLog.

        This method also takes care of calling reduce to debundle files
        if necessary. The strategy for this is as follows:

        recipe = rqe.recipe if debundle is None else 'debundle'
        reduce input files with recipe
        self.reduced_files = reduce.output_files
        if debundle == 'ALL':
            input_files = reduce.output_files # from debundle step
            reduce input_files with rqe.recipe
            self.reduced_files = reduce.output_files
        elif debundle == 'INDIVIDUAL':
            self.reduced_files = []
            input_files = reduce.output_files # from debundle step
            for input_file in input_files:
                reduce input_file with rqe.recipe
                self.reduced_files += reduce.output_files
        elif debundle == 'GHOST':
            # TBD
        elif ...
        # There is no else clause here
        capture self.reduced_files as normal

        We do this all with one instance of Reduce(), setting data values as
        needed and calling Reduce.runr() multiple times.

        Set self.rqe.failed via logrqeerror if fails.
        """
        # See https://dragons.readthedocs.io/projects/recipe-system-users-manual/en/v3.2.3/appendices/full_api_example.html#api-example

        # Add the DRAGONS customizations (ie additional log levels) to logger
        customize_logger(self.l)

        # Configure the cal manager to only fetch things from the archive.
        configstring = f"""
[calibs]
databases = {self.fsc.reduce_calibs_url} get
"""

        # If we're using a reduce calibration cache, set it here
        if self.reduce_calcache_dir:
            configstring += (f"system_calcache_dir = {self.reduce_calcache_dir}\n"
                             f"system_calcache_gbs = {self.reduce_calcache_gbs}")

        dragons_config = cal_service.globalConf
        dragons_config.read_string(configstring)

        # Instantiate the ProcessingLog record with initial values from the rqe
        processinglog = ProcessingLog(self.rqe)
        self.s.add(processinglog)
        self.s.commit()

        # Start the log capture for the processing log at this point
        logcapture = io.StringIO()
        handler = logging.StreamHandler(stream=logcapture)
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)-8s - %(message)s')
        handler.setFormatter(formatter)
        handler.setLevel(15)  # DRAGONS FULLINFO level
        self.l.addHandler(handler)

        self.l.info("DRAGONS [calibs] Configuration:")
        for key in dragons_config['calibs']:
            self.l.info(f">>>    {key} : {dragons_config['calibs'][key]}")

        # Instantiate Reduce()
        try:
            reduce = Reduce()
        except Exception as e:
            self.logrqeerror(f"Exception instantiating Reduce(): {e}",
                             exc_info=True)
            # Capture log and close down log capture
            self.l.removeHandler(handler)
            processinglog.end(self.reduced_files, self.rqe.failed)
            self.s.commit()  # Ensure transaction on rqe is closed
            # Add the captured log output and close the logcapture StringIO
            processinglog.log = logcapture.getvalue()
            logcapture.close()
            self.s.commit()
            return

        # Set mode. It defaults to sq, but we need to over-ride to qa for the
        # instrument monitoring recipes. By convention, these start with
        # check - eg checkBias1
        reduce.mode = 'qa' if (self.rqe.recipe is not None and
                               self.rqe.recipe.startswith('check')) else 'sq'

        # Tell Reduce() what files to reduce
        reduce.files.extend(self.rqe.filenames)

        # Tell Reduce() not to upload calibrations. We do that ourselves here.
        reduce.upload = None

        # Are we debundling or specifying a recipe name?
        if self.rqe.debundle:
            reduce.recipename = 'processBundle'
            self.l.info(f"Specifying recipe name processBundle for Reduce()")
        elif self.rqe.recipe:
            self.l.info(f"Specifying recipe name {self.rqe.recipe} for "
                        f"Reduce()")
            reduce.recipename = self.rqe.recipe

        # Are we passing any uparms?
        if self.rqe.uparms:
            try:
                uparms = ast.literal_eval(self.rqe.uparms)
            except Exception as e:
                self.logrqeerror(f"Exception in parsing uparms "
                                 f"{self.rqe.uparms}: {e}", exc_info=True)
                # Capture log and close down log capture
                self.l.removeHandler(handler)
                processinglog.end(self.reduced_files, self.rqe.failed)
                self.s.commit()  # Ensure transaction on rqe is closed
                # Add the captured log output and close the logcapture StringIO
                processinglog.log = logcapture.getvalue()
                logcapture.close()
                self.s.commit()
                return
        else:
            uparms = {}

        # If we are setting any primitive parameters from config, so do now
        if self.fsc.reducer_stackframes_memory:
            uparms['stackFrames:memory'] = self.fsc.reducer_stackframes_memory

        # chdir into the working directory for DRAGONS. Store the current
        # working dir so we can go back after
        pwd = os.getcwd()
        os.chdir(self.workingdir)

        debundle = self.rqe.debundle
        recipe = self.rqe.recipe
        self.s.commit()  # Ensure transaction on rqe is closed

        # Ensure we don't have an old value left over
        self.actual_recipe = None

        try:
            # If we're not debundling, this is *the* Reduce call. If we are
            # debundling, this is the debundle and the main Reduce call follows.
            # We do not specify uparms on debundle reduce calls as it barfs
            reduce.uparms = {} if debundle else uparms
            self.l.info("Calling DRAGONS Reduce.runr() "
                        f"in directory {self.workingdir} "
                        f"with recipe {reduce.recipename} "
                        f"and uparms {reduce.uparms}")
            reduce.runr()
            self.l.info("DRAGONS Reduce.runr() appeared to complete successfully")
            self.l.info("Recipe name was: %s", reduce.recipename)
            self.l.info("Output filenames are: %s", reduce.output_filenames)
            self.l.info("Processed filenames are: %s", reduce.processed_filenames)
            # If the recipe calls storeProcessedCheese, the filenames of the
            # processed cheese are in reduce.processed_filenames. If it doesn't,
            # then we use reduce.output_filenames, which is the content of the
            # main stream at the end of the recipe. The debundle recipe for
            # example does not storeProcessedCheese, and the processing
            # following a debundle uses reduce.output_filenames directly.
            self.reduced_files = self._get_reduce_outputs(reduce, debundle)
            self.l.info("Reduced files are: %s", self.reduced_files)
            self.actual_recipe = reduce.recipename
            # If we're debundling, need to handle further calls to reduce here
            if debundle == 'ALL':
                self.reduced_files = []
                reduce.files = reduce.output_filenames
                reduce._output_filenames = []
                reduce.recipename = recipe if recipe else "_default"
                reduce.uparms = uparms
                self.l.info("Debundle ALL - Calling DRAGONS Reduce.runr() in "
                            f"directory {self.workingdir} "
                            f"with recipe {reduce.recipename} "
                            f"and uparms {reduce.uparms} "
                            f"on: {reduce.files}")
                reduce.runr()
                self.l.info("DRAGONS Reduce.runr() appeared to complete successfully")
                self.l.info("Recipe name was: %s", reduce.recipename)
                self.l.info("Output filenames are: %s", reduce.output_filenames)
                self.l.info("Processed filenames are: %s", reduce.processed_filenames)
                self.reduced_files = self._get_reduce_outputs(reduce, debundle)
                self.l.info("Reduced files are: %s", self.reduced_files)
                self.actual_recipe = reduce.recipename
            elif debundle == 'INDIVIDUAL':
                self.reduced_files = []
                input_files = reduce.output_filenames
                reduce.uparms = uparms
                for input_file in input_files:
                    reduce.recipename = recipe if recipe else "_default"
                    reduce.files = [input_file]
                    reduce._output_filenames = []
                    self.l.info("Debundle INDIVIDUAL - Calling DRAGONS "
                                f"Reduce.runr() in directory {self.workingdir} "
                                f"with recipe {reduce.recipename} "
                                f"and uparms {reduce.uparms} "
                                f"on {reduce.files}")
                    reduce.runr()
                    self.l.info("DRAGONS Reduce.runr() appeared to complete successfully")
                    self.l.info("Recipe name was: %s", reduce.recipename)
                    self.l.info("Output filenames are: %s", reduce.output_filenames)
                    self.l.info("Processed filenames are: %s", reduce.processed_filenames)
                    self.reduced_files.extend(
                        self._get_reduce_outputs(reduce, debundle))
                self.l.info("All Reduced files are: %s", self.reduced_files)
                self.actual_recipe = reduce.recipename
            elif debundle and debundle.startswith('GHOST'):
                self.reduced_files = []
                reduce.uparms = uparms
                # Which arms do we want?
                if debundle == 'GHOST-SLIT':
                    cameras = ['slit']
                elif debundle == 'GHOST-REDBLUE':
                    cameras = ['red', 'blue']
                else:
                    cameras = ['slit', 'red', 'blue']
                # Sort the output filenames into desired lists
                dict_of_lists = {'red': [], 'blue': [], 'slit': []}
                for filename in reduce.output_filenames:
                    for camera in cameras:
                        thing = '_' + camera
                        if thing in filename:
                            dict_of_lists[camera].append(filename)
                # Call reduce on each of the lists
                for camera in cameras:
                    reduce.recipename = recipe if recipe else "_default"
                    reduce.files = dict_of_lists[camera]
                    reduce._output_filenames = []
                    self.l.info("Debundle GHOST - Calling DRAGONS "
                                f"Reduce.runr() in directory {self.workingdir} "
                                f"with recipe {reduce.recipename} "
                                f"and uparms {reduce.uparms} "
                                f"on {reduce.files}")
                    reduce.runr()
                    self.l.info("DRAGONS Reduce.runr() appeared to complete successfully")
                    self.l.info("Recipe name was: %s", reduce.recipename)
                    self.l.info("Output filenames are: %s", reduce.output_filenames)
                    self.l.info("Processed filenames are: %s", reduce.processed_filenames)
                    self.reduced_files.extend(
                        self._get_reduce_outputs(reduce, debundle))
                self.l.info("All Reduced files are: %s", self.reduced_files)
                self.actual_recipe = reduce.recipename
            elif debundle:
                self.l.error(f"Debundle strategy {debundle} "
                             "not implemented in Reducer.call_reduce()")


        except Exception as e:
            self.logrqeerror(f"Exception in do_reduce, likely from "
                             f"DRAGONS Reduce.runr(): {e}", exc_info=True)
            # Capture log and close down log capture
            self.l.removeHandler(handler)
            processinglog.end(self.reduced_files, self.rqe.failed)
            self.s.commit()  # Ensure transaction on rqe is closed
            # Add the captured log output and close the logcapture StringIO
            processinglog.log = logcapture.getvalue()
            logcapture.close()
            self.s.commit()
            os.chdir(pwd)
            return

        os.chdir(pwd)

        # As of 2025-06-17 DRAGONS Reduce() appears to create cyclic object
        # references, or otherwise causes the effects of a memory leak.
        # Calling gc.collect() seems to mitigate this.
        # We also check our memory footprint after calling for garbage
        # collection, and issue a warning if it seems high.
        gc.collect()
        rss = psutil.Process().memory_info().rss
        # 400000000 (400MB) seems typical on linux, python 3.12, once things
        # are underway.
        if rss > 600000000:
            self.l.warning(f'Reducer memory footprint excessive: {rss} bytes')

        # In some situations, the memory footprint just keeps growing, so we need
        # to trigger a restart at this point. We still have work to do on the
        # current reduction though, so we check this (again) at the end of do_reduce()
        # and raise an exception there to trigger service_reduce_queue to bail out.

        processinglog.end(self.reduced_files, self.rqe.failed)
        self.s.commit()  # Ensure transaction on rqe is closed

        self.l.info("At end of call_reduce, reduced files are: %s",
                    self.reduced_files)

        # Terminate log capture
        self.l.removeHandler(handler)

        # Add the captured log output and close the logcapture StringIO
        processinglog.log = logcapture.getvalue()
        logcapture.close()

        self.s.commit()

    def _get_reduce_outputs(self, reduce, debundle=None):
        # Convenience method to deal with reduce .output_filenames and
        # .processed_filenames
        retary = reduce.processed_filenames
        if not reduce.processed_filenames:
            if not debundle:
                self.l.warning("reduce.processed_filenames was empty after "
                               "a non-debundle recipe. Using "
                               "reduce.output_filenames instead.")
            else:
                self.l.debug("reduce.processed_filenames empty, capturing "
                             "reduce.output_filenames instead")
            retary = reduce.output_filenames
        self.l.info(f"Reduced Files: {retary}")
        return retary
