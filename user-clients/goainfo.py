import urllib
import json
import argparse
import datetime

parser = argparse.ArgumentParser()
parser.add_argument("selection", help="Data selection (eg filename) to query")
args = parser.parse_args()

# Construct the URL. We'll use the jsonsummary service
url = "https://archive.gemini.edu/jsonfilelist/"

url += args.selection

# Open the URL and decode the JSON
u = urllib.urlopen(url)
files = json.load(u)
u.close()

# This is a list of dictionaries each containing info about a file
print "%20s %32s %8s" % ("Name", "Last modified", "Metadata")

for f in files:
    md = 'OK' if f['mdready'] else 'BAD'
    print "%20s %32s %8s" % (f['name'], f['lastmod'], md)
