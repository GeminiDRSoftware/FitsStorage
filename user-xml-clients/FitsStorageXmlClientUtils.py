import urllib2
from xml.dom.minidom import parseString
import hashlib

def get_file_list(selection, canonical=True, present=True, server='fits'):
  # This function takes a selection string, for example "GMOS/Imaging/today"
  # and returns a list describing the files that match. Each entry in the list
  # is a dictionary containing 'filename', 'size', 'md5' and 'lastmod' elements

  url = "http://%s/xmlfilelist" % server

  if(canonical):
    url+="/canonical"

  if(present):
    url+="/present"

  url+="/"+selection

  u = urllib2.urlopen(url)
  xml = u.read()
  u.close()

  dom = parseString(xml)
  files = []
  for fe in dom.getElementsByTagName("file"):
    dict = {}
    dict['filename']=fe.getElementsByTagName("filename")[0].childNodes[0].data
    dict['size']=int(fe.getElementsByTagName("size")[0].childNodes[0].data)
    dict['md5']=fe.getElementsByTagName("md5")[0].childNodes[0].data
    dict['lastmod']=fe.getElementsByTagName("lastmod")[0].childNodes[0].data
    files.append(dict)

  return files

def fetch_files(files, server='fits'):
  # given a list of files in the format from get_file_list above
  # This function fetches the files, checking the md5sums
  # of the data received against what the server said they should be

  # loop through the list fetching the files, using gemini staff access
  # Check the md5sum of what we got matches what the database thinks it should be
  # Please do not distribute this code to non gemini staff
  for file in files:
    url = "http://%s/file/%s" % (server, file['filename'])
    opener = urllib2.build_opener()
    opener.addheaders.append(('Cookie', 'gemini_fits_authorization=good_to_go'))
    #print "Fetching: %s" % url
    u = opener.open(url)
    data = u.read()
    u.close()

    # Compute the md5
    m = hashlib.md5()
    m.update(data)
    md5 = m.hexdigest()

    # Compare against database value
    # If OK, write file, if not, complain
    if(md5==file['md5']):
      print "fetched file %s" % file['filename']
      f = open(file['filename'], 'w')
      f.write(data)
      f.close()
    else:
      print "md5 did not match, problem with file %s" % file['filename']

def get_cal_list(selection, caltype, server='fits'):
    """
    Fetches a list of calibraitons from the calibration manager
    on the fits server
    """
    url = "http://%s/calmgr" % server

    url+="/"+selection

    u = urllib2.urlopen(url)
    xml = u.read()
    u.close()

    dom = parseString(xml)
    cals = []
    for fe in dom.getElementsByTagName("calibration"):
      dict = {}
      dict['filename']=fe.getElementsByTagName("filename")[0].childNodes[0].data
      dict['datalabel']=fe.getElementsByTagName("datalabel")[0].childNodes[0].data
      dict['caltype']=fe.getElementsByTagName("caltype")[0].childNodes[0].data
      dict['md5']=fe.getElementsByTagName("md5")[0].childNodes[0].data
      dict['url']=fe.getElementsByTagName("url")[0].childNodes[0].data
      cals.append(dict)

    return cals

