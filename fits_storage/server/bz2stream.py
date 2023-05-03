"""
This module contains a streaming bz2 compressor. Bizarrely, the standard
library and bz2 module don't contain anything equivalent - they provide means
open a bz2 file and provide a file-like-object to read and write plain text
data, but not the other way round, which is what we're doing here.
"""

import bz2

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
    """
    def __init__(self, src, chunksize=1000000):
        """
        Instantiate a StreamBz2Compressor instance.
        src - file-like-object to read data from
        chunksize (default 1MB) size of the chunks of data we will compress
        """
        self.src = src
        self.comp = bz2.BZ2Compressor()
        self.output_buffer = bytes(0)
        self.done = False
        self.chunksize = chunksize

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

        return ret
