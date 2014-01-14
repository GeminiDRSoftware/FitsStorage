"""
This is the hashes module. It provides a convenience interface to hashing.
Currently, the only hash function we use is md5sum

"""
import hashlib
import gzip

def md5sum_size_fp(f):
  """
  Generates the md5sum and size of the data returned by the file-like object f, returns 
  a tuple containing the hex string md5 and the size in bytes.
  f must be open. It will not be closed. We will read from it until we encounter EOF. 
  No seeks will be done, f will be left at eof
  """
  # This is the block size by which we read chunks from the file, in bytes
  block = 1000000 # 1 MB

  m = hashlib.md5()
 
  size = 0

  data = f.read(block)
  size += len(data)
  m.update(data)
  while(data):
    data = f.read(block)
    size += len(data)
    m.update(data)

  return (m.hexdigest(), size)

def md5sum(filename):
  """
  Generates the md5sum of the data in filename, returns the hex string.
  """

  f = open(filename, 'r')
  (m, s) = md5sum_size_fp(f)
  f.close()
  return m

def md5sum_size_gz(filename):
  """
  Generates the md5sum and size of the uncompressed data in a gzip file
  """

  f = gzip.open(filename, 'rb')
  (m, s) = md5sum_size_fp(f)
  f.close()
  return (m, s)
