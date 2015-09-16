import urllib
import json

# Construct the URL. We'll use the jsonsummary service
url = "https://archive.gemini.edu/jsonsummary/"

# List the OBJECT files taken with GMOS-N on 2010-12-31
url += "canonical/OBJECT/GMOS-N/20101231"

# Open the URL and fetch the JSON document text into a string
u = urllib.urlopen(url)
jsondoc = u.read()
u.close()

# Decode the JSON
files = json.loads(jsondoc)

# This is a list of dictionaries each containing info about a file
total_data_size = 0
print "%20s %22s %10s %8s %s" % ("Filename", "Data Label", "ObsClass", 
                                 "QA state", "Object Name")

for f in files:
    total_data_size += f['data_size']
    print "%20s %22s %10s %8s %s" % (f['name'], f['data_label'], 
                                     f['observation_class'], f['qa_state'], 
                                     f['object'])

print "Total data size: %d" % total_data_size
