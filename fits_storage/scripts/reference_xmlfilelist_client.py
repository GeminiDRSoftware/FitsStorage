import urllib.request, urllib.parse, urllib.error
from xml.dom.minidom import parseString

# Data selection. For example:
selection = "today/GMOS/BIAS"

url = "http://fits/xmlfilelist/" + selection
xml = urllib.request.urlopen(url).read()
dom = parseString(xml)

def getXmlData(element, tag):
    return element.getElementsByTagName(tag)[0].childNodes[0].data

def getFileDataFromXml(fe):
    return {
        'filename': getXmlData(fe, 'filename'),
        'size':     getXmlData(fe, 'size'),
        'lastmod':  getXmlData(fe, 'lastmod')
    }

files = [getFileDataFromXml(fe) for fe in dom.getElementsByTagName('file')]

# files is now a list, where each element in the list is a dictionary representing a fits file, and having 'filename', 'size', 'lastmod' etc keys.

numfiles = len(files)
print("Got %d files" % numfiles)

for f in files:
    print("Filename: %s     size: %d     last_modification: %s" % (f['filename'], f['size'], f['lastmod']))
