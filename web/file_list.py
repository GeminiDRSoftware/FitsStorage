"""
This is the Fits Storage Web Summary module. It provides the functions
which query the database and generate html for the web header
summaries.
"""
import json

from orm import sessionfactory
from web.summary import list_headers
from web.standards import xmlstandardobs
import apache_return_codes as apache

def xmlfilelist(req, selection):
    """
    This generates an xml list of the files that met the selection
    """
    req.content_type = "text/xml"
    req.write('<?xml version="1.0" ?>')
    req.write("<file_list>")
    req.write("<selection>%s</selection>" % selection)

    session = sessionfactory()
    orderby = ['filename_asc']
    try:
        headers = list_headers(session, selection, orderby)
        for header in headers:
            req.write("<file>")
            req.write("<filename>%s</filename>" % header.diskfile.filename)
            req.write("<size>%d</size>" % header.diskfile.file_size)
            req.write("<md5>%s</md5>" % header.diskfile.file_md5)
            req.write("<lastmod>%s</lastmod>" % header.diskfile.lastmod)
            if(header.phot_standard):
                xmlstandardobs(req, header.id)
            req.write("</file>")
    finally:
        session.close()
    req.write("</file_list>")
    return apache.OK


def jsonfilelist(req, selection):
    """
    This generates a JSON list of the files that met the selection
    """
    req.content_type = "application/json"

    session = sessionfactory()
    orderby = ['filename_asc']
    try:
        headers = list_headers(session, selection, orderby)
        thelist = []
        for header in headers:
            thedict = {}
            thedict['filename'] = str(header.diskfile.filename)
            thedict['size'] =  str(header.diskfile.file_size)
            thedict['md5'] = str(header.diskfile.file_md5)
            thedict['lastmod'] = str(header.diskfile.lastmod)
            thelist.append(thedict)
    finally:
        session.close()
    json.dump(thelist, req, indent=4)
    return apache.OK

