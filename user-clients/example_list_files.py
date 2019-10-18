import urllib.request, urllib.parse, urllib.error
import json

# Construct the URL. We'll use the jsonfilelist service
url = "https://archive.gemini.edu/jsonfilelist/"

# List the files for GN-2010B-Q-22 taken with GMOS-N on 2010-12-31
url += "canonical/GN-2010B-Q-22/GMOS-N/20101231"

# Open the URL and fetch the JSON document text into a string
u = urllib.request.urlopen(url)
jsondoc = u.read()
u.close()

# Decode the JSON
files = json.loads(jsondoc)

# This is a list of dictionaries each containing info about a file
for f in files:
    print("Filename: %s" % f['filename'])
    print("-- file size: %d, data size: %d" % (f['file_size'], f['data_size']))

