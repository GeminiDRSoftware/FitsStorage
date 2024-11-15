import astropy.io.fits as pf
import os
import re
import json

from datetime import datetime, timedelta

from fits_storage.logger import DummyLogger
from fits_storage.config import get_config

"""
Utilities for cleaning up headers from visitor instruments.

The visiting instruments have had trouble with providing us
our requested header keywords.  As much as we'd prefer that
they create datafiles to the spec, we don't want to hold up
support for their data over it.  So, these utilities look
for known issues in the files and correct for them.

Eventually, the hope is these methods would see compliant
datafiles and not have to make any changes.
"""


def fix_zorro_or_alopeke(fits, instr, telescope):
    """
    Method to clean up FITS files from Zorro or Alopeke.

    Parameters
    ----------
    fits : astropy.io.fits hdulist
        File to read data from
    instr : str
        Which instrument, `zorro` or `alopeke`
    telescope : str
        Which telescope

    Returns
    -------
    True if we needed to make any fixes, False if the file is fine as it is
    """
    retval = False
    pheader = fits[0].header
    if 'INSTRUME' not in pheader:
        pheader['INSTRUME'] = instr.upper()
        retval = True
    if 'Object' in pheader and 'OBJECT' not in pheader:
        pheader['OBJECT'] = pheader['Object']
        del pheader['Object']
    if 'OBSTYPE' in pheader and pheader['OBSTYPE'] is not None:
        pheader['OBSTYPE'] = pheader['OBSTYPE'].upper()
    if 'GEMPRGID' in pheader:
        if 'OBSID' not in pheader or pheader['OBSID'] == pheader['GEMPRGID']:
            pheader['OBSID'] = "%s-0" % pheader['GEMPRGID']
            retval = True
        if 'DATALAB' not in pheader:
            pheader['DATALAB'] = "%s-0" % pheader['OBSID']
            retval = True
    if 'CTYPE1' in pheader:
        val = pheader['CTYPE1']
        if val == 'RA--TAN':
            pheader['CTYPE1'] = 'RA---TAN'
            retval = True
    if 'CTYPE2' in pheader:
        val = pheader['CTYPE2']
        if val == 'RA--TAN':
            pheader['CTYPE2'] = 'RA---TAN'
            retval = True
    if 'CRVAL1' in pheader:
        val = pheader['CRVAL1']
        if isinstance(val, str):
            pheader['CRVAL1'] = float(val)
            retval = True
    if 'CRVAL2' in pheader:
        val = pheader['CRVAL2']
        if isinstance(val, str):
            pheader['CRVAL2'] = float(val)
            retval = True
    if 'EXPTIME' in pheader:
        val = pheader['EXPTIME']
        if isinstance(val, str):
            pheader['EXPTIME'] = float(val)
            retval = True
    if 'OBSTYPE' not in pheader:
        pheader['OBSTYPE'] = 'OBJECT'
        retval = True
    if 'OBSCLASS' not in pheader:
        pheader['OBSCLASS'] = 'science'
        retval = True
    if 'TELESCOP' not in pheader:
        pheader['TELESCOP'] = telescope
        retval = True
    # per Andrew S, we always update the RELEASE keyword, it was not reliably
    # being set correctly
    if 'DATE-OBS' in pheader and pheader['DATE-OBS'] is not None:
        try:
            dateobs = pheader['DATE-OBS']
            dt = datetime.strptime(dateobs, '%Y-%m-%d')
            if 'CAL' in pheader['OBSID']:
                pheader['RELEASE'] = dt.strftime('%Y-%m-%d')
            elif '-FT-' in pheader['OBSID'] or '-DD-' in pheader['OBSID']:
                pheader['RELEASE'] = (dt + timedelta(days=183)).\
                    strftime('%Y-%m-%d')
            elif pheader['OBSID'] in ['GN-2024B-DD-101', 'GS-2024B-DD-101']:
                # These Steve Howell DD programs should be immediately public
                # per email 2024-01-09
                pheader['RELEASE'] = dt.strftime('%Y-%m-%d')
            else:
                pheader['RELEASE'] = (dt + timedelta(days=365)).\
                    strftime('%Y-%m-%d')
        except Exception:
            # print("Unable to determine release date, continuing")
            pass
    return retval


def fix_igrins(fits):
    """
    Fix IGRINS datafile.

    Parameters
    ----------
    fits : astropy.io.fits hdulist

    Returns
    -------
    True if we had to modify the file, False if it was fine
    """
    pheader = fits[0].header
    if 'INSTRUME' not in pheader:
        return False
    inst = pheader['INSTRUME']
    if inst.strip() != 'IGRINS':
        return False
    retval = False
    if 'TELESCOP' in pheader and pheader['TELESCOP'].lower() == 'gemini south':
        pheader['TELESCOP'] = 'Gemini-South'
        retval = True
    if 'OBSERVAT' not in pheader or pheader['OBSERVAT'] is None:
        pheader['OBSERVAT'] = "Gemini-South"
        retval = True
    progid = None
    if 'GEMPRGID' in pheader and pheader['GEMPRGID']:
        progid = pheader['GEMPRGID']
    elif 'GEMPRID' in pheader and pheader['GEMPRID']:
        progid = pheader['GEMPRID']
        pheader['GEMPRGID'] = pheader['GEMPRID']
    elif 'PROGID' in pheader and pheader['PROGID']:
        progid = pheader['PROGID']
        pheader['GEMPRGID'] = pheader['PROGID']
    if progid is not None:
        if 'OBSID' not in pheader or pheader['OBSID'] == progid:
            pheader['OBSID'] = "%s-0" % progid
            retval = True
        elif 'OBSID' in pheader and isinstance(pheader['OBSID'], int):
            obsid = pheader['OBSID']
            pheader['OBSID'] = "%s-%s" % (progid, obsid)
        if 'DATALAB' not in pheader:
            pheader['DATALAB'] = "%s-0" % pheader['OBSID']
            retval = True
    # fix RELEASE header if missing, we base this on DATE-OBS + 1 year
    if 'RELEASE' not in pheader and 'DATE-OBS' in pheader \
            and pheader['DATE-OBS'] is not None:
        try:
            dateobs = pheader['DATE-OBS']
            if len(dateobs) >= 10:
                dateobs = dateobs[0:10]
                dt = datetime.strptime(dateobs, '%Y-%m-%d')
                pheader['RELEASE'] = (dt + timedelta(days=365))\
                    .strftime('%Y-%m-%d')
                retval = True
            else:
                # print("RELEASE not set and DATE-OBS in unrecognized format")
                pass
        except Exception:
            # print("Unable to determine release date, continuing")
            pass
    return retval


DATE_CRE = re.compile(r'20\d\d[01]\d[0123]\d')
fsc = get_config()
vi_staging_path = json.loads(fsc.vi_staging_paths)


class VisitorInstrumentHelper(object):
    def __init__(self, staging_dir=None, dest_dir=None, logger=None):
        # Staging dir is the "root" directory of the visiting instrument
        # staging area. This dir should contain subdirectories by date with
        # the files from that date inside.
        self.staging_dir = staging_dir

        # dest_dir is the "root" directory of dataflow
        self.dest_dir = fsc.storage_root if dest_dir is None else dest_dir

        self.logger = logger if logger is not None else DummyLogger()

        # if defined, this is a compiled regular expression that should match
        # the raw filenames from this instrument. This is defined in the
        # subclasses
        self.filename_cre = None

        self.subdir = None

        self.instrument_name = None

    def fixheader(self, hdulist):
        self.logger.error('fixheader() called on generic VI helper instance')

    def file_exists(self, filename):
        """
        Check if the file exists in dest_dir / subdir and is readable,
        with or without a .bz2

        Returns True if file exists, is a file, and is readable
        """

        dest_path = os.path.join(self.dest_dir, self.subdir, filename)
        if os.access(dest_path, os.F_OK | os.R_OK):
            return True

        if filename.endswith('.bz2'):
            fn = filename.removesuffix('.bz2')
            dest_path = os.path.join(self.dest_dir, self.subdir, fn)
            if os.access(dest_path, os.F_OK | os.R_OK):
                return True
        else:
            fn = filename + '.bz2'
            dest_path = os.path.join(self.dest_dir, self.subdir, fn)
            if os.access(dest_path, os.F_OK | os.R_OK):
                return True
        return False

    def too_new(self, filename):
        """
        Get the lastmod time of filename in the source directory.
        If within 5 seconds of now, return True, else False
        """

        src_path = os.path.join(self.staging_dir, self.subdir, filename)
        lastmod = datetime.fromtimestamp(os.path.getmtime(src_path))

        age = datetime.now() - lastmod
        age = age.total_seconds()
        if age < 5:
            self.logger.debug("%s is too new (%.1f)- skipping this time round",
                              filename, age)
            return True
        else:
            return False

    def fix_and_copy(self, filename, compress=None):
        """
        Fix and copy the visitor instrument file 'filename'. You should have
        already set the subdir to work in by calling subdir() on this instance.

        If compress is None, the output file will be compressed if the input
        file is. If compress is True or False, that will determine whether we
        compress the output file.

        Returns True on success, False on Failure

        """
        src_path = os.path.join(self.staging_dir, self.subdir, filename)

        if compress is None:
            dest_filename = filename
        else:
            if compress:
                # Ensure the destination filename ends in .bz2
                dest_filename = filename if filename.endswith('.bz2') else \
                    filename + '.bz2'
            else:
                # Ensure the destination filename does not end in .bz2
                dest_filename = filename.rstrip('.bz2')
        dest_path = os.path.join(self.dest_dir, self.subdir, dest_filename)

        # Ensure destination directory exists
        dd = os.path.join(self.dest_dir, self.subdir)
        if not (os.path.isdir(dd) and os.access(dd, os.W_OK)):
            os.mkdir(dd)

        self.logger.info("Copying %s file: %s", self.instrument_name, filename)
        self.logger.debug("Reading visitor instrument data from: %s", src_path)
        self.logger.debug("Writing visitor instrument data to: %s", dest_path)

        try:
            hdulist = pf.open(src_path, do_not_scale_image_data=True,
                              mode='readonly')

            if self.fixheader(hdulist):
                self.logger.debug("Applied %s header fixes",
                                  self.instrument_name)
                hdulist[0].header['HISTORY'] = f'Corrected metadata: ' \
                                               f'{self.instrument_name} fixes'

            hdulist.writeto(dest_path, output_verify='silentfix+exception')
            return True
        except Exception:
            self.logger.error("Exception copying and fixing Visitor Instrument "
                              "file %s", filename, exc_info=True)
            if os.path.exists(dest_path):
                os.unlink(dest_path)
            return False

    def list_files(self):
        # List the files in self.subdir that match the filename format for this
        # instrument
        dirpath = os.path.join(self.staging_dir, self.subdir)
        for i in os.listdir(dirpath):
            if self.filename_cre is None:
                yield i
            elif self.filename_cre.match(i):
                self.logger.debug("Filename %s matches instrument filename_cre",
                                  i)
                yield i
            else:
                self.logger.debug("Filename %s does not match instrument "
                                  "filename_cre - skipping", i)
                pass

    def list_datedirs(self):
        # Generate a list of the YYYYMMDD "date" directories in staging_dir
        for i in os.listdir(self.staging_dir):
            if DATE_CRE.match(i):
                yield i


class AlopekeVIHelper(VisitorInstrumentHelper):
    def __init__(self, staging_dir=None, dest_dir=None, logger=None):
        super(AlopekeVIHelper, self).__init__(staging_dir=staging_dir,
                                              dest_dir=dest_dir, logger=logger)

        if self.staging_dir is None:
            self.staging_dir = vi_staging_path.get('ALOPEKE')

        self.filename_cre = re.compile(
            r'^N20\d\d[01]\d[0123]\dA\d\d\d\d[br].fits(.bz2)?')

        self.instrument_name = 'ALOPEKE'
        self.dest_dir = os.path.join(self.dest_dir, 'alopeke')

    def fixheader(self, hdulist):
        return fix_zorro_or_alopeke(hdulist, 'ALOPEKE', 'Gemini-North')


class ZorroVIHelper(VisitorInstrumentHelper):
    def __init__(self, staging_dir=None, dest_dir=None, logger=None):
        super(ZorroVIHelper, self).__init__(staging_dir=staging_dir,
                                            dest_dir=dest_dir, logger=logger)

        if self.staging_dir is None:
            self.staging_dir = vi_staging_path.get('ZORRO')

        self.filename_cre = re.compile(
            r'^S20\d\d[01]\d[0123]\dZ\d\d\d\d[br].fits(.bz2)?')

        self.instrument_name = 'ZORRO'
        self.dest_dir = os.path.join(self.dest_dir, 'zorro')

    def fixheader(self, hdulist):
        return fix_zorro_or_alopeke(hdulist, 'ZORRO', 'Gemini-South')


class IGRINSVIHelper(VisitorInstrumentHelper):
    def __init__(self, staging_dir=None, dest_dir=None, logger=None):
        super(IGRINSVIHelper, self).__init__(staging_dir=staging_dir,
                                             dest_dir=dest_dir, logger=logger)

        if self.staging_dir is None:
            self.staging_dir = vi_staging_path.get('IGRINS')

        self.filename_cre = re.compile(
            r'^SDC[HKS]_20\d\d[01]\d[0123]\d_\d{4}.fits(.bz2)?')

        self.instrument_name = 'IGRINS'
        self.dest_dir = os.path.join(self.dest_dir, 'igrins')

    def fixheader(self, hdulist):
        return fix_igrins(hdulist)
