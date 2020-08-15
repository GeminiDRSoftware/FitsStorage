import astropy.io.fits as pf
from bz2 import BZ2File
import os
from argparse import ArgumentParser
from datetime import datetime, timedelta, date

from fits_storage.scripts.emailutils import sendmail


"""
Utilities for cleaning up headers from unreliable sources.

The visiting instruments have had trouble with providing us
our requested header keywords.  As much as we'd prefer that
they create datafiles to the spec, we don't want to hold up
support for their data over it.  So, these utilities look
for known issues in the files and correct for them.

Eventually, the hope is these methods would see perfect
datafiles and not have to make any changes.
"""


def open_image(path):
    """
    Open the given datafile.

    This opens the provided datafile.  If it is a `.bz2` file,
    it will wrap it appropriately.

    Parameters
    ----------
    path : str
        Path to file

    Returns
    -------
    file-like object
    """
    if path.endswith('.bz2'):
        return BZ2File(path)

    return open(path, 'rb')


def output_file(path):
    """
    Create writeable file handle for given path.

    This creates a file-like object for writing out to.  If
    we are writing a `.bz2` file, it encapsulates the compression
    for us.
    """
    if path.endswith('.bz2'):
        return BZ2File(path, 'wb')

    return open(path, 'wb')


def fix_zorro_or_alopeke(fits, instr, telescope):
    """
    Method to clean up FITS files from Zorro or Alopeke.

    Parameters
    ----------
    fits : `~Astrodata`
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
    if 'TELESCOP' not in pheader:
        pheader['TELESCOP'] = telescope
        retval = True
    # per Andrew S, we always update the RELEASE keyword, it was not reliably being set
    if 'DATE-OBS' in pheader and pheader['DATE-OBS'] is not None:
        try:
            dateobs = pheader['DATE-OBS']
            dt = datetime.strptime(dateobs, '%Y-%m-%d')
            if 'CAL' in pheader['OBSID']:
                pheader['RELEASE'] = dt.strftime('%Y-%m-%d')
            elif '-FT-' in pheader['OBSID']:
                pheader['RELEASE'] = (dt + timedelta(days=183)).strftime('%Y-%m-%d')
            else:
                pheader['RELEASE'] = (dt + timedelta(days=365)).strftime('%Y-%m-%d')
        except Exception as e:
            print("Unable to determine release date, continuing")
    return retval


def fix_zorro(fits):
    """
    Fix Zorro datafile.

    This just wraps the call to `fix_zorro_or_alopeke` with the
    appropriate instrument and telescope.

    Parameters
    ----------
    fits : astrodata
        Astrodata object with Zorro data to check

    Returns
    -------
    True if we had to modify the file, False if it was fine
    """
    return fix_zorro_or_alopeke(fits, 'Zorro', 'Gemini-South')


def fix_alopeke(fits):
    """
    Fix Alopeke datafile.

    This just wraps the call to `fix_zorro_or_alopeke` with the
    appropriate instrument and telescope.

    Parameters
    ----------
    fits : astrodata
        Astrodata object with Alopeke data to check

    Returns
    -------
    True if we had to modify the file, False if it was fine
    """
    return fix_zorro_or_alopeke(fits, 'Alopeke', 'Gemini-North')


def fix_igrins(fits):
    """
    Fix IGRINS datafile.

    Parameters
    ----------
    fits : astrodata
        Astrodata object with IGRINS data to check

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
    progid = None
    if 'GEMPRGID' in pheader:
        progid = pheader['GEMPRGID']
    elif 'GEMPRID' in pheader:
        progid = pheader['GEMPRID']
        pheader['GEMPRGID'] = pheader['GEMPRID']
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
    if 'RELEASE' not in pheader and 'DATE-OBS' in pheader and pheader['DATE-OBS'] is not None:
        try:
            dateobs = pheader['DATE-OBS']
            if len(dateobs) >= 10:
                dateobs = dateobs[0:10]
                dt = datetime.strptime(dateobs, '%Y-%m-%d')
                pheader['RELEASE'] = (dt + timedelta(days=365)).strftime('%Y-%m-%d')
                retval = True
            else:
                print("RELEASE not set and DATE-OBS in unrecognized format")
        except Exception as e:
            print("Unable to determine release date, continuing")
    return retval


def fix_and_copy(src_dir, dest_dir, fn, compress=True, mailfrom=None, mailto=None):
    """
    Fix and copy the given file from the visiting instrument staging folder to
    the appropriate dataflow location.

    Parameters
    ----------
    src_dir : str
        Location of file in the visitor instrument staging area
    dest_dir : str
        Location in dataflow to put the file with any required fixes
    fn : str
        Name of the file
    compress : bool
        If True, we should compress the file on dataflow
    mailfrom : str
        If set, use this as the FROM address for any alert emails
    mailto : str
        If set, send any alert emails to this address
    """
    path = os.path.join(src_dir, fn)
    tmppath = None
    if fn.endswith('.bz2'):
        tmppath = os.path.join('/tmp/', fn[:-4])
        os.system('bzcat %s > %s' % (path, tmppath))

    df = os.path.join(dest_dir, fn)
    if df.endswith('.bz2') and not compress:
        df = df[:-4]

    try:
        if tmppath:
            fits = pf.open(open_image(tmppath), do_not_scale_image_data=True)
        else:
            fits = pf.open(open_image(path), do_not_scale_image_data=True)
        if 'zorro' in dest_dir.lower():
            if fix_zorro(fits):
                fits[0].header['HISTORY'] = 'Corrected metadata: Zorro fixes'
        if 'alopeke' in dest_dir.lower():
            if fix_alopeke(fits):
                fits[0].header['HISTORY'] = 'Corrected metadata: Alopeke fixes'
        if fix_igrins(fits):
            fits[0].header['HISTORY'] = 'Corrected metadata: IGRINS fixes'
        fits.writeto(output_file(df), output_verify='silentfix+exception')
    except (IOError, ValueError) as e:
        if mailfrom and mailto:
            message = ["ERROR - Unable to fix visiting instrument data",
                       "file: %s" % fn,
                       "(from %s to %s)" % (src_dir, dest_dir)]
            sendmail("ERROR - Unable to import visiting instrument file %s" % fn,
                     mailfrom, mailto, message)
        print('{0} >> {1}'.format(fn, e))
        if os.path.exists(df):
            os.unlink(df)
    finally:
        if tmppath is not None:
            os.unlink(tmppath)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--src", action="store", type=str, dest="src_dir",
                        help="Source directory")
    parser.add_argument("--dest", action="store", type=str, dest="dest_dir",
                        help="Destination directory")
    parser.add_argument("--compress", action="store_true", default=False, dest="compress",
                        help="Compress output file as .bz2")
    parser.add_argument('path', nargs='+', help='Path of a file or a folder of files.')
    options = parser.parse_args()
    src_dir = options.src_dir
    dest_dir = options.dest_dir
    compress = options.compress

    if src_dir == dest_dir:
        print('destination cannot be the same as the source')
    files = options.path

    for fn in files:
        fix_and_copy(src_dir, dest_dir, fn, compress=compress)
