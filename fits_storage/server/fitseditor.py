import astropy.io.fits
import bz2
import shutil
import tempfile
from time import strptime

from sqlalchemy.exc import NoResultFound, MultipleResultsFound

from fits_storage.logger_dummy import DummyLogger
from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header
from fits_storage.core.hashes import md5sum
from fits_storage.config import get_config

qa_states = {
    'undefined': {'RAWGEMQA': 'UNKNOWN', 'RAWPIREQ': 'UNKNOWN'},
    'pass':      {'RAWGEMQA': 'USABLE',  'RAWPIREQ': 'YES'},
    'usable':    {'RAWGEMQA': 'USABLE',  'RAWPIREQ': 'NO'},
    'fail':      {'RAWGEMQA': 'BAD',     'RAWPIREQ': 'NO'},
    'check':     {'RAWGEMQA': 'CHECK',   'RAWPIREQ': 'CHECK'}
}

rawsite_states = {
    'bg20':  {'RAWBG': '20-percentile'},
    'bg50':  {'RAWBG': '50-percentile'},
    'bg80':  {'RAWBG': '80-percentile'},
    'bgany': {'RAWBG': 'Any'},
    'cc50':  {'RAWCC': '50-percentile'},
    'cc70':  {'RAWCC': '70-percentile'},
    'cc80':  {'RAWCC': '80-percentile'},
    'ccany': {'RAWCC': 'Any'},
    'iq20':  {'RAWIQ': '20-percentile'},
    'iq70':  {'RAWIQ': '70-percentile'},
    'iq85':  {'RAWIQ': '85-percentile'},
    'iqany': {'RAWIQ': 'Any'},
    'wv20':  {'RAWWV': '20-percentile'},
    'wv50':  {'RAWWV': '50-percentile'},
    'wv80':  {'RAWWV': '80-percentile'},
    'wvany': {'RAWWV': 'Any'},
}


class FitsEditor(object):
    """
    A class to abstract modification of FITS headers in files we're curating.
    Instantiate the class with either a filename or datalabel, then call
    methods to modify the headers. The class will take care of all the details
    such as S3, bz2, etc.

    This class can be used as a context manager.
    """
    searchkey = None
    diskfile = None
    error = None
    message = None
    localfile = None
    hdulist = None
    s3 = None
    s3_tmpdir = None

    def __init__(self, filename=None, datalabel=None, session=None,
                 logger=DummyLogger(), do_setup=True):
        self.filename = filename
        self.datalabel = datalabel
        self.session = session
        self.logger = logger
        self.error = False
        self.message = ''
        if filename is None and datalabel is None:
            self.error = True
            self.message = "Must provide either filename or datalabel. "
            self.logger.error(self.message)
            return

        # This is here to support alternate setups for testing.
        if do_setup:
            self._find_diskfile()
            self._get_localfile()
            self._get_hdulist()

    def __enter__(self, filename=None, datalabel=None, logger=DummyLogger()):
        self.__init__(filename=filename, datalabel=datalabel, logger=logger)
        return self

    def __exit__(self):
        self.close()

    def _find_diskfile(self):
        query = self.session.query(File).join(DiskFile).join(Header)\
            .filter(DiskFile.canonical == True)
        if self.filename is not None:
            query = query.filter(File.name ==
                                 self.filename.removesuffix('.bz2'))
        else:
            query = query.filter(Header.data_label == self.datalabel)

        try:
            self.file = query.one()
            self.diskfile = self.session.query(DiskFile)\
                .filter(DiskFile.file_id == self.file.id)\
                .filter(DiskFile.present == True).one()
        except NoResultFound:
            self.error = True
            self.message = f'No results searching for target file ' \
                           f'({self.filename} or {self.datalabel}). '
            self.logger.error(self.message)
        except MultipleResultsFound:
            self.error = True
            self.message = f'Multiple results searching for target file ' \
                           f'({self.filename} or {self.datalabel}). '
            self.logger.error(self.message)
        if self.diskfile is not None and self.diskfile.present is False:
            self.error = True
            self.message = f'Target diskfile is not present ' \
                           f'(id: {self.diskfile.id} - ' \
                           f'filename:{self.diskfile.filename}). '
            self.logger.error(self.message)

        if self.diskfile is None:
            self.logger.warning("FitsEditor could not find diskfile")
        else:
            self.logger.debug("FitsEditor found diskfile id %d:%s/%s",
                              self.diskfile.id, self.diskfile.path,
                              self.diskfile.filename)

    def _get_localfile(self):
        fsc = get_config()

        if self.error is True:
            return

        # Use the existing diskfile infrastructure to provide us an
        # uncpompressed version of the file, as astropy.io.fits handling
        # of .bz2 files cannot handle mode = 'update'

        # Note, we need to take care of deleting the uncompressed_cache_file
        # once we're done! We do this by calling diskfile.cleanup()

        # We need to set these values here for get_uncompressed_file etc.
        # to work as __init__ hasn't been called on this diskfile instance.
        # However, these may have been set by the testing framework

        # When using S3, we fetch the file from S3 into a private temporary
        # directory and point the diskfile storage_root at that, so that all
        # the code below (and in close()) works the same as the local case.
        if fsc.using_s3:
            if not self._fetch_from_s3():
                return
        else:
            self.diskfile._storage_root = fsc.storage_root if \
                self.diskfile._storage_root is None else \
                self.diskfile._storage_root

        self.diskfile._z_staging_dir = fsc.z_staging_dir if \
            self.diskfile._z_staging_dir is None else \
            self.diskfile._z_staging_dir

        self.diskfile._logger = self.logger

        # Note, this will return the path to the diskfile itself if it is not
        # compressed. If it is compressed, it's a tmpfile containing the
        # uncompressed data.
        self.diskfile.get_uncompressed_file(compute_values=False)
        self.localfile = self.diskfile.uncompressed_cache_file

        if self.localfile is None:
            self.error = True
            self.message = "Failed to create local uncompressed cache file"
            self.logger.error(self.message)
        else:
            self.logger.debug("FitsEditor editing local file: %s",
                              self.localfile)

    def _fetch_from_s3(self):
        """
        Fetch the file from S3 into a private temporary directory inside
        s3_staging_dir, and set the diskfile storage_root to point there.
        Returns True on success, False (and sets self.error) on failure.
        """
        from fits_storage.server.aws_s3 import Boto3Helper
        fsc = get_config()
        self.s3 = Boto3Helper(logger=self.logger)

        # Use a private directory so that we can't collide with the
        # ingester, which fetches to s3_staging_dir/path/filename and
        # deletes anything already there.
        try:
            self.s3_tmpdir = tempfile.mkdtemp(dir=fsc.s3_staging_dir,
                                              prefix='fitseditor_')
        except OSError:
            self.error = True
            self.message += 'Failed to create temporary directory in ' \
                            f'{fsc.s3_staging_dir}. '
            self.logger.error(self.message, exc_info=True)
            return False
        self.diskfile._storage_root = self.s3_tmpdir

        keyname = self.diskfile.keyname
        self.logger.debug("FitsEditor fetching %s from S3 to %s",
                          keyname, self.diskfile.fullpath)
        if not self.s3.fetch_to_storageroot(keyname,
                                            fullpath=self.diskfile.fullpath,
                                            skip_tests=True):
            self.error = True
            self.message += f'Failed to fetch {keyname} from S3. '
            self.logger.error(self.message)
            return False

        # Check that we got the version of the file that the database knows
        # about. Note, this also works for old S3 objects that don't have an
        # md5 in their metadata.
        local_md5 = md5sum(self.diskfile.fullpath)
        if local_md5 != self.diskfile.file_md5:
            self.error = True
            self.message += f'S3 object {keyname} md5 {local_md5} does not ' \
                            f'match database md5 {self.diskfile.file_md5} - ' \
                            f'maybe a newer version has not been ingested ' \
                            f'yet? '
            self.logger.error(self.message)
            return False

        return True

    def _upload_to_s3(self):
        """
        Upload the edited file at diskfile.fullpath back to S3, under the same
        key. Sets self.error on failure.
        """
        keyname = self.diskfile.keyname

        # upload_file() only sets the md5 and extra_meta metadata, so carry
        # over any other existing metadata. upload_file() calculates and sets
        # the new md5 itself - this is what the ingester uses to detect that
        # the file has changed.
        try:
            meta = dict(self.s3.get_key(keyname).metadata or {})
        except Exception:
            self.logger.warning("Could not read existing S3 metadata for %s",
                                keyname, exc_info=True)
            meta = {}
        meta.pop('md5', None)

        self.logger.info("FitsEditor uploading %s to S3", keyname)
        try:
            obj = self.s3.upload_file(keyname, self.diskfile.fullpath,
                                      extra_meta=meta)
        except Exception:
            self.logger.error("Exception uploading to S3", exc_info=True)
            obj = None
        if obj is None:
            self.error = True
            self.message += f'S3 upload of {keyname} failed. '
            self.logger.error(self.message)

    def _get_hdulist(self):
        if self.error is True:
            return
        try:
            # Note, the file we're not operating on - self.localfile - is
            # either the diskfile file itself, or the diskfile
            # uncompressed_cache_file
            self.hdulist = astropy.io.fits.open(self.localfile, mode='update',
                                                do_not_scale_image_data=True)
        except Exception:
            self.error = True
            self.message = 'Error opening file with astropy.io.fits. '
            self.logger.error(self.message, exc_info=True)

        self.logger.debug("FitsEditor hdulist: %s", self.hdulist)

    def close(self):
        fsc = get_config()

        if self.hdulist is not None:
            # Remember, the file behind the hdulist is either the diskfile file
            # itself in storage_root, or the diskfile uncompressed_cache_file

            # If the diskfile was compressed, we're using a cache file, and we
            # now need to write a compressed copy back in place of the original
            # (compressed) file in the storage_root
            if self.diskfile.compressed:
                # Write the hdulist to the compressed file.
                # Note, in astropy 6.x, io.fits can write the bz2 file directly.
                # In the S3 case, this is the local copy in s3_tmpdir.
                self.logger.info("FitsEditor writing bz2 compressed file: %s",
                                 self.diskfile.fullpath)
                with bz2.open(self.diskfile.fullpath, mode='wb') as f:
                    self.hdulist.writeto(f)

            # Note, this calls hdulist.flush() as we are in mode = 'update'.
            # The flush is pointless if we're in compressed mode as we just
            # wrote it all out to a new file anyway, but we still need to close
            # the uncompressed_cache_file and it's cheap to flush as it's
            # uncompressed.
            # If we're not in compressed mode, this is the actual file update
            # with the changes we made going directly to the fits file.
            # TODO - check if this is still the case in astropy 6.x
            self.logger.debug("Closing hdulist: %s", self.hdulist.filename())
            self.hdulist.close()

            # The edited file is now complete at diskfile.fullpath. If we're
            # using S3, upload it back. Don't upload if there was an error,
            # so a bad request leaves the file in S3 untouched.
            if fsc.using_s3 and not self.error:
                self._upload_to_s3()

        # If we were using an uncompressed_cache_file, delete it now. Note that
        # this can be the case even if self.hdulist is None - ie we failed to
        # instantiate an hdulist for some reason.

        # This deletes the uncompressed_cache_file if we are using one.
        if self.diskfile is not None:
            self.diskfile.cleanup()

        # If we fetched the file from S3, delete our local copy
        if self.s3_tmpdir is not None:
            self.logger.debug("Deleting S3 temporary directory %s",
                              self.s3_tmpdir)
            shutil.rmtree(self.s3_tmpdir, ignore_errors=True)
            self.s3_tmpdir = None

    def set_qa_state(self, qa_state):
        if qa_state is None or (qa_state := qa_state.lower()) not in \
                qa_states.keys():
            self.error = True
            self.message += f'Invalid QA state: {qa_state}. '
            self.logger.error(self.message)
            return False
        for keyword in qa_states[qa_state]:
            self.hdulist[0].header[keyword] = qa_states[qa_state][keyword]
        return True

    def set_release(self, release):
        try:
            # This will raise ValueError in vase of an illegal date
            strptime(release, "%Y-%m-%d")
        except (ValueError, TypeError):
            self.error = True
            self.message += f'Invalid Release date: {release}. '
            return False
        self.hdulist[0].header['RELEASE'] = release
        return True

    def set_rawsite(self, rawsite):
        if rawsite is None or (rawsite := rawsite.lower()) not in \
                rawsite_states.keys():
            self.error = True
            self.message += f'Invalid Raw Site Quality state: {rawsite}. '
            return False
        for keyword in rawsite_states[rawsite]:
            self.hdulist[0].header[keyword] = rawsite_states[rawsite][keyword]
        return True

    def set_header(self, keyword, value, ext=0, reject_new=False):
        if keyword not in self.hdulist[ext].header and reject_new:
            self.error = True
            self.message += f"keyword {keyword} is new and reject_new = True. "
            return False
        if len(keyword) > 8:
            self.error = True
            self.message += f"Invalid keyword: {keyword}. "
            return False
        self.hdulist[ext].header[keyword] = value
        return True
