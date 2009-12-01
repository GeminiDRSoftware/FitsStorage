import FitsStorage
import FitsStorageUtils
import os
import re

from optparse import OptionParser

# Get a list of all the files in the datastore
# We assume this is just one dir (ie non recursive) for now.

path=''

fulldirpath = os.path.join(FitsStorage.storage_root, path)
print "Ingesting files from: ", fulldirpath

parser = OptionParser()
parser.add_option("--force-crc", action="store_true", dest="force_crc", help="Force crc check on pre-existing files")
parser.add_option("--file-re", action="store", type="string", dest="file_re", help="python regular expression string to select files by")

(options, args) = parser.parse_args()

filelist = os.listdir(fulldirpath)

cre=re.compile(options.file_re)

files=[]
if(options.file_re):
  for filename in filelist:
    if(cre.search(filename)):
      files.append(filename)
else:
  files = filelist

i=0
n=len(files)
for filename in files:
  i+=1
  print "-- Ingesting (%d/%d): %s" % (i, n, filename)
  FitsStorageUtils.ingest_file(filename, path, options.force_crc)
