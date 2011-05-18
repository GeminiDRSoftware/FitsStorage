"""
This is the Fits Storage Web Summary module. It provides the functions
which query the database and generate html for the web header
summaries.
"""
from FitsStorageWebSummary.Summary import *
from FitsStorageWebSummary.Standards import xmlstandardobs


def xmlfilelist(req, selection):
  """
  This generates an xml list of the files that met the selection
  """
  req.content_type = "text/xml"
  req.write('<?xml version="1.0" ?>')
  req.write("<file_list>")

  session = sessionfactory()
  orderby = ['filename_asc']
  selection['present']=True
  try:
    headers = list_headers(session, selection, orderby)
    for h in headers:
      req.write("<file>")
      req.write("<filename>%s</filename>" % h.diskfile.file.filename)
      req.write("<size>%d</size>" % h.diskfile.size)
      req.write("<md5>%s</md5>" % h.diskfile.md5)
      req.write("<ccrc>%s</ccrc>" % h.diskfile.ccrc)
      req.write("<lastmod>%s</lastmod>" % h.diskfile.lastmod)
      xmlstandardobs(req, h.id)
      req.write("</file>")
  finally:
    session.close()
  req.write("</file_list>")
  return apache.OK
