import urllib
from xml.dom.minidom import parseString

# For example:
selection = "today/GMOS/BIAS"

url = "http://fits/xmlfilelist/" + selection

u = urllib.urlopen(url)
xml = u.read()
u.close()

dom = parseString(xml)
files = []
for fe in dom.getElementsByTagName("file"):
  dict = {}
  dict['filename']=fe.getElementsByTagName("filename")[0].childNodes[0].data
  dict['size']=int(fe.getElementsByTagName("size")[0].childNodes[0].data)
  dict['ccrc']=fe.getElementsByTagName("ccrc")[0].childNodes[0].data
  dict['lastmod']=fe.getElementsByTagName("lastmod")[0].childNodes[0].data
  files.append(dict)

numfiles = len(files)

print "Got %d files" % numfiles
