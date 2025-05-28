"""
This module contains code for reducing data with DRAGONS
"""
import io
import logging
import os
import os.path
import shutil
import bz2
import json
import http
import requests

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

from fits_storage.core.hashes import md5sum
from fits_storage.server.bz2stream import StreamBz2Compressor

# DRAGONS imports
from recipe_system.reduction.coreReduce import Reduce
from gempy.utils.logutils import customize_logger
from recipe_system import cal_service

from fits_storage.config import get_config
if get_config().using_s3:
    from fits_storage.server.aws_s3 import Boto3Helper

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
        self.using_s3 = self.fsc.using_s3
        self.upload_url = self.fsc.reducer_upload_url

        # We store the reducequeueentry passed for reduction in the class for
        # convenience and to facilitate the seterror convenience function
        self.rqe = rqe

        # Initialize these to None here, they are set as they are used
        self.workingdir = None

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

        if not self.rqe.failed:
            self.makeworkingdir()

        if not self.rqe.failed:
            self.getrawfiles()

        if not self.rqe.failed:
            self.call_reduce()

        if not self.rqe.failed:
            self.set_reduction_metadata()

        if not self.rqe.failed:
            if self.rqe.capture_files:
                self.capture_reduced_files()
            else:
                self.l.info("Not capturing Output files from this processing "
                            "run")

        if not self.rqe.failed:
            if self.rqe.capture_monitoring:
                self.capture_monitoring()
            else:
                self.l.info("Not capturing monitoring values from this "
                            "processing run")

        if self.nocleanup or (self.fsc.fits_system_status == 'development'
                              and self.rqe.failed):
            self.l.warning("Not cleaning up working directory")
        else:
            self.cleanup()

        if self.rqe.failed:
            return False

        # Yay. If we got here, we successfully reduced the data.
        self.l.debug("Deleting competed rqe id %d", self.rqe.id)
        self.s.delete(self.rqe)

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
        for filename in self.rqe.filenames:
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

                for kw in ['PROCSOFT', 'PROCSVER', 'PROCMODE', 'PROCITNT',
                           'PROCINBY', 'PROCTAG']:
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
                dst = os.path.join(dstpath, filename)
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
                                        args={"filename": filename,
                                              "processed_cal": False,
                                              "path": self.rqe.tag,
                                              "fileuploadlog_id": None})

                foq.add(fo_req, filename=filename, response_required=False)


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

            # Construct upload URL.
            dst = os.path.join(self.rqe.tag, destination_filename)
            url = f"{self.upload_url}/{dst}"
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

            try:
                fpfn = os.path.join(self.workingdir, filename)
                ad = astrodata.open(fpfn)
                # This simplistic approach won't be viable in the long term.
                if filename.endswith("_biasCorrected_snr.fits"):
                    # Capture bias values
                    for slice in ad:
                        for keyword in ('OVERSCAN', 'OVERRMS', 'PIXMEAN',
                                        'PIXSTDEV', 'SNRMEAN', 'FSNRGT3',
                                        'PIXMED'):
                            mon = Monitoring(slice)
                            mon.keyword = keyword
                            mon.label = slice.amp_read_area()
                            mon.header_id = self.header_id
                            self.s.add(mon)
                            self.s.commit()
                # Quote-unquote close the astrodata instance
                ad = None
            except Exception:
                self.l.warning("Exception capturing BIAS monitoring data",
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
        Add an entry to ProcessingLog

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
            configstring += f"system_calcache_dir = {self.reduce_calcache_dir}"

        dragons_config = cal_service.globalConf
        dragons_config.read_string(configstring)

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

        # Call Reduce()
        try:
            reduce = Reduce()
        except Exception:
            self.logrqeerror("Exception instantiating Reduce()", exc_info=True)
            return

        # Tell Reduce() what files to reduce
        reduce.files.extend(self.rqe.filenames)

        # Tell Reduce() not to upload calibrations. We do that ourselves here.
        reduce.upload = None

        # Are we specifying a recipe name?
        if self.rqe.recipe:
            self.l.info(f"Specifying recipe name {self.rqe.recipe} for "
                        f"Reduce()")
            reduce.recipename = self.rqe.recipe

        # chdir into the working directory for DRAGONS. Store the current
        # working dir so we can go back after
        pwd = os.getcwd()

        self.l.info("Calling DRAGONS Reduce.runr() in directory %s",
                    self.workingdir)
        os.chdir(self.workingdir)

        # Instantiate the ProcessingLog record with initial values from the rqe
        processinglog = ProcessingLog(self.rqe)
        self.s.add(processinglog)
        self.s.commit()
        try:
            reduce.runr()
        except Exception:
            self.logrqeerror("Exception from DRAGONS Reduce.runr()",
                             exc_info=True)
            return
        finally:
            os.chdir(pwd)

        self.reduced_files = reduce.output_filenames

        processinglog.end(len(self.reduced_files), self.rqe.failed)

        self.l.info("DRAGONS Reduce.runr() appeared to complete successfully")
        self.l.info("Output files are: %s", reduce.output_filenames)

        # Terminate log capture
        self.l.removeHandler(handler)

        # Add the captured log output and close the logcapture StringIO
        processinglog.log = logcapture.getvalue()
        logcapture.close()

        self.s.commit()
