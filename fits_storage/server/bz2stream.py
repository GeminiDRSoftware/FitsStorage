"""
This module contains a streaming bz2 compressor. Bizarrely, the standard
library and bz2 module don't contain anything equivalent - they provide means
open a bz2 file and provide a file-like-object to read and write plain text
data, but not the other way round, which is what we're doing here.
"""

import bz2
import hashlib


class StreamBz2Compressor(object):
    """
    This class provides a somewhat file-like-object from which you can read
    bz2 compressed data that is compressed from the source file given when
    you instantiate the class.

    Instantiate this object by passing it an open and positioned file-like
    object from which to read the data to be compressed. The only method this
    function calls on this object is .read(size).

    You can then call .read(size) on this object and that will return the
    source data after bz2 compressing it. The only file-like method this class
    supports is .read(size).

    This class provides two extra properties:
        * bytes_output which gives the number of bytes that have been read
        from the class so far.
        * md5sum_output which gives the md5sum of the data output so far.
    These update as data is read from the class, they are most useful after
    all the data have been read of course.
    """
    def __init__(self, src, chunksize=1000000, itersize=1000000):
        """
        Instantiate a StreamBz2Compressor instance.
        src - file-like-object to read data from
        chunksize (default 1MB) size of the chunks of data we will compress
        itersize (default 1MB) size of the chunks we return as an iterator
        """
        self.src = src
        self.comp = bz2.BZ2Compressor()
        self.output_buffer = bytes(0)
        self.done = False
        self.chunksize = chunksize
        self.itersize = itersize
        self._bytes_output = 0
        self.hashobj = hashlib.md5()

    def _fill_buffer(self, request_size):
        """
        Attempt to fill the output_buffer to contain at least request_size
        bytes of bz2 compressed data. If there are not that many bytes
        available, fill the buffer with whatever is available.
        """
        while not self.done and len(self.output_buffer) < request_size:
            chunk = self.src.read(self.chunksize)
            if not chunk:
                comp = self.comp.flush()
                if comp:
                    self.output_buffer += comp
                self.done = True
            else:
                comp = self.comp.compress(chunk)
                if comp:
                    self.output_buffer += comp

    def read(self, n):
        """
        Return at most n bytes of data. If no more data is available, return
        None.
        """
        # If we don't have enough data in the output buffer, top it up to the
        # number of bytes requested
        if len(self.output_buffer) < n:
            self._fill_buffer(n)

        # If the output buffer is empty, even after the top-up, we're at EOF.
        if not self.output_buffer:
            return None

        # If we get here, we have sufficient data in the output buffer to
        # satisfy the request. Return the first n bytes of the buffer and
        # remove those bytes from the buffer.
        ret = self.output_buffer[:n]
        self.output_buffer = self.output_buffer[n:]

        self._bytes_output += len(ret)
        self.hashobj.update(ret)
        return ret

    @property
    def bytes_output(self):
        return self._bytes_output

    @property
    def md5sum_output(self):
        return self.hashobj.hexdigest()

    # requests uses the presence of __iter__ to determine that the thing
    # passed is a file-like-object, and itterates the object to retrieve
    # the contents.
    def __iter__(self):
        return self

    def __next__(self):
        chunk = self.read(self.itersize)
        if chunk is None:
            raise StopIteration
        return chunk
