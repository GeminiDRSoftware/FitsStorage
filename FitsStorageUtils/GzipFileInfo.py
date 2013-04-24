"""
Some gzip utilities. Some of these may be obsoleted by a more up to date version of the python gzip module
"""

import struct
import datetime
from gzip import FEXTRA, FNAME, FEXTRA, FCOMMENT, FHCRC

def read_gzip_info(filename):
  """
  Read the gzip header from the file with given filename
  Returns a dictionary {size, filename, mtime, lastmod, comment, crc} read from the header
  """

  size = None
  fname = None
  mtime = None
  comment = None 
  crc = None

  gf = open(filename, mode='readonly')

  # The following is somewhat copied from gzip.py _read_gzip_header

  magic = gf.read(2)
  if magic != '\037\213':
    raise IOError, 'Not a gzipped file'

  # Compression method
  method = struct.unpack("<B", gf.read(1))[0]
  if method != 8:
    raise IOError, 'Unknown compression method: %d' % method

  # Flags
  flag = ord(gf.read(1))

  mtime = struct.unpack("<I", gf.read(4))[0]
  extraflag = gf.read(1)
  os = gf.read(1)

  if flag & FEXTRA:
    # Read & discard the extra field, if present
    xlen = ord(gf.read(1))
    xlen = xlen + 256*ord(gf.read(1))
    gf.read(xlen)
  if flag & FNAME:
    # Read a null-terminated string containing the filename
    fname=""
    while True:
      s = gf.read(1)
      if not s or s=='\000':
        break
      else:
        fname+=s
  if flag & FCOMMENT:
    # Read a null-terminated string containing a comment
    comment=""
    while True:
      s = gf.read(1)
      if not s or s=='\000':
        break
      else:
        comment+=s
  if flag & FHCRC:
    # Read the 16-bit header CRC
    crc = struct.unpack("<h", gf.read(2))[0]

  # Read archive size
  gf.seek(-4, 2)
  size = struct.unpack('<I', gf.read())[0]

  gf.close()

  dict = {'size':size, 'filename':fname, 'mtime':mtime, 'lastmod':datetime.datetime.fromtimestamp(mtime), 'comment':comment, 'crc':crc}

  return dict
