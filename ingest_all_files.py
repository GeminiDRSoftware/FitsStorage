import FitsStorage
import FitsStorageUtils
import os

# Get a list of all the files in the datastore
# We assume this is just one dir (ie non recursive) for now.

path=''

fulldirpath = os.path.join(FitsStorage.storage_root, path)
print "Ingesting all files in: ", fulldirpath

filelist = os.listdir(fulldirpath)
n=len(filelist)

i=0
for filename in filelist:
  i+=1
  print "-- Ingesting (%d/%d): %s" % (i, n, filename)
  FitsStorageUtils.ingest_file(filename, path)
