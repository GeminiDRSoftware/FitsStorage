"""
This is the hashes module. It provides a convenience interface to hashing.
Currently, the only hash function we use is md5sum

"""
import hashlib
import bz2


__all__ = ["md5sum"]


def md5sum(filename):
    """
    Generates the md5sum of the data in filename, returns the hex string.

    Parameters
    ----------
    filename : str
        File name of an uncompressed file

    Returns
    -------
    str, int : md5 sum and size of the data
    """

    with open(filename, 'rb') as filep:
        block = 1000000  # 1MB
        hashobj = hashlib.md5()
        while True:
            data = filep.read(block)
            if not data:
                break
            hashobj.update(data)

        return hashobj.hexdigest()
