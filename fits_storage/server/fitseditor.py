import astropy.io.fits
import bz2
from time import strptime

from sqlalchemy.exc import NoResultFound, MultipleResultsFound

from fits_storage.logger import DummyLogger
from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header
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

        if fsc.using_s3:
            self.error = True
            self.message = 'S3 header updates not implemented yet. '
            self.logger.error(self.message)
            return

        # Use the existing diskfile infrastructure to provide us an
        # uncpompressed version of the file, as astropy.io.fits handling
        # of .bz2 files cannot handle mode = 'update'

        # Note, we need to take care of deleting the uncompressed_cache_file
        # once we're done! We do this by calling diskfile.cleanup()

        # We need to set these values here for get_uncompressed_file etc.
        # to work as __init__ hasn't been called on this diskfile instance.
        # However, these may have been set by the testing framework

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

        if fsc.using_s3:
            # Will have errored as not implemented on open
            return

        if self.hdulist is not None:
            # Remember, the file behind the hdulist is either the diskfile file
            # itself in storage_root, or the diskfile uncompressed_cache_file

            # If the diskfile was compressed, we're using a cache file, and we
            # now need to write a compressed copy back in place of the original
            # (compressed) file in the storage_root
            if self.diskfile.compressed:
                # Write the hdulist to the compressed file.
                # Note, in astropy 6.x, io.fits can write the bz2 file directly.
                # Note this is not the S3 case
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

        # If we were using an uncompressed_cache_file, delete it now. Note that
        # this can be the case even if self.hdulist is None - ie we failed to
        # instantiate an hdulist for some reason.

        # This deletes the uncompressed_cache_file if we are using one.
        self.diskfile.cleanup()

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
