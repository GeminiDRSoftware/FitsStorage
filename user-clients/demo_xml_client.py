from FitsStorageXmlClientUtils import get_file_list

# Data selection. For example:
selection = "today/GMOS/Imaging/OBJECT"

files = get_file_list(selection)

# files is now a list, where each element in the list is a dictionary 
# representing a fits file, and having 'filename', 'size', 'md5', 'lastmod' keys.
# Here's some examples of how you access that information

numfiles = len(files)
print "Got %d files" % numfiles

for file in files:
  print "Filename: %s   size: %d" % (file['filename'], file['size'])

