from FitsStorageXmlClientUtils import get_file_list, fetch_files

# Data selection. For example:
selection = "yesterday/GMOS/Imaging/OBJECT/"

files = get_file_list(selection)
# files is now a list, where each element in the list is a dictionary representing a fits file, and having 'filename', 'size', 'md5', 'lastmod' keys.

numfiles = len(files)
print "Got %d files\n" % numfiles

# Fetch the files
fetch_files(files)
