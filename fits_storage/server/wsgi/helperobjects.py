"""
This module contains various "helper" objects used by the response object.
Some of them implement buffering, others file-type specific helpers.
"""

# TODO - cgi.FieldStorage is being deprecated soon, needs to be replaced.
# TODO - are these really necessary?
# Some of these could be simply got rid of I think, others could be replaced
# by things like io.StringIO or io.BytesIO or similar?

from cgi import FieldStorage
import json
import os


# Boilerplate object. Maybe later we'll add something else to it?
class UploadedFile(object):
    def __init__(self, name):
        self.name = name

    def rename_to(self, path):
        pass


class ItemizedFieldStorage(FieldStorage):
    # def __init__(self, fp, environ):
    def __init__(self, fp=None, headers=None, outerboundary=b'',
                 environ=os.environ, keep_blank_values=0, strict_parsing=0,
                 limit=None, encoding='utf-8', errors='replace',
                 max_num_fields=None, separator=None):

        FieldStorage.__init__(self, fp, headers=headers, outerboundary=outerboundary,
                              environ=environ, keep_blank_values=keep_blank_values,
                              strict_parsing=strict_parsing, limit=limit, encoding=encoding,
                              errors=errors, max_num_fields=max_num_fields)
        if self.filename is None:
            self.uploaded_file = None
        else:
            self.uploaded_file = UploadedFile(self.filename)

    def items(self):
        for k in list(self.keys()):
            yield (k, self[k])

    def iteritems(self):
        return list(self.items())


class StreamingObject(object):
    """
    Helper file-like object that implements a buffered output. Useful as a
    target for json.dump and other functions producing large outputs that
    need to be streamed.

    A :py:class:`StreamingObject` will buffer the data written to it up to a
    certain limit, dumping the buffer to a certain output when it reaches its
    limit.
    """
    def __init__(self, callback, buffer_size=0):
        """
        ``buffer_size`` is the threshold that needs to be reached before
        dumping the contents of the buffer. Size 0 means no buffering.

        :py:class:`StreamingObject` is output agnostic. It is initialized
        with a ``callback`` that will be invoked passing the buffer contents
        as a string. This callback is responsible for delivering the buffer
        to the output.
        """
        self._callback = callback
        self._maxbuffer = buffer_size
        self._reset_buffer()

    def write(self, data):
        self._buffer.append(data)
        self._buffered += len(data)
        if self._buffered > self._maxbuffer:
            self.flush()

    def _reset_buffer(self):
        self._buffer = []
        self._buffered = 0

    def flush(self):
        """
        Dump the buffer contents right away.
        """
        buffer = self._buffer
        if len(self._buffer) > 0 and isinstance(self._buffer[0], bytes):
            for b in self._buffer:
                self._callback(b)
        else:
            self._callback(''.join(buffer).encode('utf8'))
        self._reset_buffer()

    def close(self):
        """
        Does nothing, except calling :py:meth:`StreamingObject.flush`
        """
        self.flush()

