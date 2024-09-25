

"""
Ricardo brought to my attention that the WCS headers for Zorro/Alopeke were broken.
CTYPEx was set to RA--TAN instead of RA---TAN.  Rather than reingest everything
after patching headers, it seemed better to write this script to:

a) query Zorro/Alopeke files for a given date/range
b) pull each file down from S3 via boto
c) update the header on that file
d) push the file back into S3 via boto

And run this on archive to avoid transfer costs
"""
import os
from datetime import datetime, timedelta

import astropy.io.fits as pf
from bz2 import BZ2File

from gemini_obs_db.db import session_scope
from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.header import Header
#from fits_storage.scripts.header_fixer2 import open_image
from gemini_obs_db.utils.hashes import md5sum


# from header_fixer2, use import after 2020-2 (not on archive, so copy paste)
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
        os.system('bzcat %s > %s' % (path, path[:-4]))
        #return BZ2File(path)
        return open(path[:-4], 'rb')

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


def get_files(session, instrument, prefix):
        # Get a list of all diskfile_ids marked as present
        q = session.query(DiskFile, Header) \
            .filter(Header.diskfile_id == DiskFile.id) \
            .filter(DiskFile.canonical) \
            .filter(Header.instrument == instrument.upper()) \
            .filter(DiskFile.filename.like("%s%%" % prefix))
        for df, h in q:
            yield df


def get_files_local(source, prefix):
    import glob
    return glob.iglob(f"{source}/{prefix}*.fits.bz2")


def get_file(s3, filename):
    print("fetching S3 filename: %s" % filename)
    fullname = os.path.join('/data/z_staging/repairin', filename)
    if not s3.fetch_to_staging(filename, fullname):
        print("Failed to fetch %s" % filename)
        return None

    return fullname


def patch_file(localname, dest="/data/z_staging/repairout"):
    fits = pf.open(open_image(localname), do_not_scale_image_data=True)
    pheader = fits[0].header
    fixed = False
    if 'CTYPE1' in pheader:
        val = pheader['CTYPE1']
        print("CTYPE1 = %s" % val)
        if val == 'RA--TAN':
            pheader['CTYPE1'] = 'RA---TAN'
            fixed = True
    if 'CTYPE2' in pheader:
        val = pheader['CTYPE2']
        print("CTYPE2 = %s" % val)
        if val == 'RA--TAN':
            pheader['CTYPE2'] = 'RA---TAN'
            fixed = True
    if fixed:
        bzoutfile = os.path.join(dest, os.path.basename(localname))
        outfile = bzoutfile[:-4]
        print("outfile is %s" % outfile)
        #fits.writeto(output_file(bzoutfile), output_verify='silentfix+exception')
        fits.writeto(output_file(outfile), output_verify='silentfix+exception')
        os.system('bzip2 -c %s > %s' % (outfile, bzoutfile))
            
        return (outfile, bzoutfile)
    return (None, None)


def put_file(s3, localname, filename):
    print("storing S3 filename: %s" % filename)
    s3.upload_file(filename, localname)


if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--instrument", action="store", type="string", default="none", dest="instrument", help="instrument to correct (zorro or alopeke).")
    parser.add_option("--prefix", action="store", type="string", default="", dest="prefix", help="filename prefix ([SN]YYYYMMDD).")
    parser.add_option("--source", action="store", type="string", default="s3", dest="source", help="source folder for input datafiles, or s3")
    parser.add_option("--dest", action="store", type="string", default="s3", dest="dest", help="destination for repaired datafiles, or s3")
    parser.add_option("--staging", action="store", type="string", default="none", dest="staging", help="staging area for bz2")

    (options, args) = parser.parse_args()

    instrument = options.instrument
    prefix = options.prefix
    source = options.source
    dest = options.dest
    staging = options.staging

    if source != dest and (source == "s3" or dest == "s3"):
        print("Source and destination must both be s3, or neither")
        exit(1)

    if dest != "s3" and not os.path.exists(dest):
        print("Destination folder not found, aborting")
        exit(2)

    if source == "s3" and instrument.upper() not in ('ALOPEKE', 'ZORRO'):
        print("Specify instrument (alopeke or zorro)")
        exit(3)
    elif source != "s3" and instrument != "none":
        print("Instrument should not be specified for a local disk source")
        exit(4)

    repairin = f"{staging}/repairin"
    repairout = f"{staging}/repairout"
    if not os.path.exists(repairin):
        os.mkdir(repairin)
    if not os.path.exists(repairout):
        os.mkdir(repairout)

    if source == "s3":
        print("Running instrument of: %s" % instrument)
    print("Saw prefix to run of: %s" % prefix)
    print("    source to run of: %s" % source)
    print("    dest to run of: %s" % dest)
    print("    staging to run of: %s" % staging)

    if source == "s3":
        # For now, disabling S3 to remind the user this should be done with care/testing
        print("While I expect this to still work, it is a big refactor.  If we need S3/DB updates, please retest "
              "carefully on a single file before running this wholesale.")
        exit(5)

        from fits_storage.utils.aws_s3 import get_helper
        s3 = get_helper()

        with session_scope() as session:
            try:
                for df in get_files(session, instrument, prefix):
                    filename = df.filename
                    localname = get_file(s3, filename)
                    print("Stored file in %s" % localname)
                    outfile, bzoutfile = patch_file(localname)
                    if outfile is not None:
                        print("%s (%s)\n%s (%s)\n" % (outfile, md5sum(outfile), bzoutfile, md5sum(bzoutfile)))
                        put_file(s3, bzoutfile, filename)
                        df.file_md5 = md5sum(bzoutfile)
                        df.file_size = os.path.getsize(bzoutfile)
                        df.data_md5 = md5sum(outfile)
                        df.data_size = os.path.getsize(outfile)
                        session.flush()
                        os.unlink(outfile)
                        os.unlink(bzoutfile)
                    else:
                        print("Unable to repair %s" % filename)
                    os.unlink(localname)
                    if localname.endswith('.bz2'):
                        os.unlink(localname[:-4])
            finally:
                session.commit()
    else:
        # glob filename from source directory
        for localname in get_files_local(source, prefix):
            print("Fixing file from %s" % localname)
            filename = os.path.basename(localname)
            print(filename)
            outfile, bzoutfile = patch_file(localname, dest)
            if outfile is not None:
                print("%s (%s)\n%s (%s)\n" % (outfile, md5sum(outfile), bzoutfile, md5sum(bzoutfile)))
                os.unlink(outfile)
            else:
                print("Unable to repair %s" % localname)
