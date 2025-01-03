import requests
from xml.dom.minidom import parseString


def getXmlData(element, tag):
    return element.getElementsByTagName(tag)[0].childNodes[0].data


def getFileDataFromXml(fe):
    return {
        'filename': getXmlData(fe, 'filename'),
        'size':     getXmlData(fe, 'size'),
        'lastmod':  getXmlData(fe, 'lastmod')
    }


if __name__ == "__main__":
    # Data selection. For example:
    selection = "today/GMOS/BIAS"

    url = "http://fits/xmlfilelist/" + selection
    r = requests.get(url)
    xml = r.text
    dom = parseString(xml)

    files = [getFileDataFromXml(fe) for fe in dom.getElementsByTagName('file')]

    # files is now a list, where each element in the list is a dictionary representing a fits file, and having 'filename', 'size', 'lastmod' etc keys.

    numfiles = len(files)
    print("Got %d files" % numfiles)

    for f in files:
        print("Filename: %s     size: %d     last_modification: %s" % (f['filename'], f['size'], f['lastmod']))
