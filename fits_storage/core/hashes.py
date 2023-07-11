"""
This is the hashes module. It provides a convenience interface to hashing.
Currently, the only hash function we use is md5sum

"""
import hashlib


__all__ = ["md5sum", "md5sum_size_fp"]


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


def md5sum_size_fp(fp):
    """
    Given an existing open file-like object fp, read data until EOF and
    calculate the size and md5sum. We do this in one pass for efficiency.
    Returns (size, md5sum)
    """
    block = 1000000  # 1MB
    size = 0
    hashobj = hashlib.md5()
    while True:
        data = fp.read(block)
        if not data:
            break
        size += len(data)
        hashobj.update(data)

    return size, hashobj.hexdigest()
