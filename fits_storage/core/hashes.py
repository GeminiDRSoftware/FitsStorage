"""
This is the hashes module. It provides a convenience interface to hashing.
Currently, the only hash function we use is md5sum

"""
import hashlib


__all__ = ["md5sum", "md5sum_size_fp"]


def md5sum(filename):
    """
    Generates the md5sum of the data in filename, returns the hex string.
    The file is hashed as-is, so for a compressed file this is the md5sum of
    the compressed data.

    Parameters
    ----------
    filename : str
        File name of the file to hash

    Returns
    -------
    str
        md5sum of the file, as a hex string
    """

    with open(filename, 'rb') as filep:
        return md5sum_size_fp(filep)[1]


def md5sum_size_fp(fp):
    """
    Given an existing open file-like object fp, read data until EOF and
    calculate the size and md5sum. We do this in one pass for efficiency.

    Parameters
    ----------
    fp : file-like object
        Open file-like object to read from, in binary mode

    Returns
    -------
    tuple of (int, str)
        size of the data in bytes, and md5sum of the data as a hex string.
        Note the order - size comes first.
    """
    block = 1000000  # 1MB
    size = 0
    hashobj = hashlib.md5(usedforsecurity=False)
    while data := fp.read(block):
        size += len(data)
        hashobj.update(data)

    return size, hashobj.hexdigest()
