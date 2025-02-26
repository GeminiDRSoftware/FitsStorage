"""
The remove_file() function is not used internally within the fits storage
code. It's primary use case is by the DRAGONS local calibration manager, which
needs to be able to remove files from the local calibration database.
"""
from os.path import basename

from fits_storage.core.orm.diskfile import DiskFile

def remove_file(path, session):
    # We simply set the diskfile entries to be not present and not canonical
    # here. To actually delete the rows we would have to find all the
    # referenced database entries (fulltextheader, footprints, etc., etc., etc.)
    # which is tedious and error-prone.

    # We simply match the basename of the provided path against the DiskFile
    # filename. This could be not what is intended in the unlikely scenarios
    # where there are multiple files with the same name but in different
    # directories within the storage area, or if the files are compressed.

    # Return True if we found and deleted entries, False otherwise

    filename = basename(path)

    diskfiles = (session.query(DiskFile)
                     .filter(DiskFile.filename == filename).all())

    if len(diskfiles) == 0:
        return False

    for diskfile in diskfiles:
        diskfile.present = False
        diskfile.canonical = False
        session.commit()
    return True