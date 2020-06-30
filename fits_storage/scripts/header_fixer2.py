import astropy.io.fits as pf
from bz2 import BZ2File
import os
from argparse import ArgumentParser
from datetime import datetime, timedelta, date


def open_image(path):
    if path.endswith('.bz2'):
        return BZ2File(path)

    return open(path, 'rb')


def output_file(path):
    if path.endswith('.bz2'):
        return BZ2File(path, 'wb')

    return open(path, 'wb')


def fix_zorro_or_alopeke(fits, instr, telescope):
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
    return fix_zorro_or_alopeke(fits, 'Zorro', 'Gemini-South')


def fix_alopeke(fits):
    return fix_zorro_or_alopeke(fits, 'Alopeke', 'Gemini-North')


def fix_igrins(fits):
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
    return retval


def fix_and_copy(src_dir, dest_dir, fn, compress=True):
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
