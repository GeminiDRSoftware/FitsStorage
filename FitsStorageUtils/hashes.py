"""
This is the hashes module. It provides a convenience interface to hashing.
Currently, the only hash function we use is md5sum

"""
import hashlib

def md5sumfp(f):
  """
  Generates the md5sum of the data returned by the file-like object f, returns the hex string.
  f must be open. It will not be closed. We will read from it until we encounter EOF. 
  No seeks will be done, f will be left at eof
  """
  # This is the block size by which we read chunks from the file, in bytes
  block = 1000000 # 1 MB

  m = hashlib.md5()

  data = f.read(block)
  m.update(data)
  while(data):
    data = f.read(block)
    m.update(data)

  return m.hexdigest()
def md5sum(filename):
  """
  Generates the md5sum of the data in filename, returns the hex string.
  """

  f = open(filename, 'r')
  m = md5sumfp(f)
  f.close()
  return m
