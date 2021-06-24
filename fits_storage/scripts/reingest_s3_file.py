
"""
This is a utility to make it easy to reingest a file that exists in S3 storage.

The script pulls down the file from S3 using the configured credentials.  It
then reingests the file.
"""
import os
from datetime import datetime, timedelta

import astropy.io.fits as pf
from bz2 import BZ2File

from fits_storage.orm import session_scope
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.header import Header
# from fits_storage.scripts.header_fixer2 import open_image
from fits_storage.utils.hashes import md5sum


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
        # return BZ2File(path)
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


def get_file(s3, filename):
    print("fetching S3 filename: %s" % filename)
    fullname = os.path.join('/tmp/reingest', filename)
    if not s3.fetch_to_staging(filename, fullname):
        print("Failed to fetch %s" % filename)
        return None

    return fullname


def put_file(s3, localname, filename):
    print("storing S3 filename: %s" % filename)
    s3.upload_file(filename, localname)


if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser()

    (options, args) = parser.parse_args()

    if not os.path.exists("/tmp/reingest"):
        os.mkdir("/tmp/reingest")

    prefix = options.prefix

    from fits_storage.utils.aws_s3 import get_helper
    s3 = get_helper()

    with session_scope() as session:
        try:
            for filename in args:
                localname = get_file(s3, filename)
                print("Stored file in %s" % localname)
                if outfile is not None:
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

