# This is the apache python handler
# See /etc/httpd/conf.d/python.conf
# When a request comes in, handler(req) gets called by the apache server

import sys
from gemini_metadata_utils import gemini_fitsfilename, gemini_date

from sqlalchemy import desc, func, join

from mod_python import apache
from mod_python import util

from fits_storage_config import blocked_urls, use_as_archive
from web.summary import summary
from web.file_list import xmlfilelist, jsonfilelist
from web.tapestuff import fileontape, tape, tapewrite, tapefile, taperead
from web.xml_tape import xmltape
from web.progsobserved import progsobserved
from web.gmoscal import gmoscal
from web.notification import notification
from web.calmgr import calmgr
from web.calibrations import calibrations
from web.upload_file import upload_file
from web.curationreport import curation_report
from web.standards import standardobs
from web.selection import getselection
from web.fileserver import fileserver
from web.qastuff import qareport, qametrics, qaforgui
from web.statistics import content, stats
from web.user import request_account, password_reset, request_password_reset, login, logout, whoami, change_password, staff_access
from web.userprogram import my_programs
from web.searchform import searchform

from orm import sessionfactory
from orm.file import File
from orm.diskfile import DiskFile
from orm.header import Header
from orm.ingestqueue import IngestQueue
from orm.diskfilereport import DiskFileReport
from orm.fulltextheader import FullTextHeader


import re
import datetime

# Compile regexps here

orderbycre = re.compile('orderby\=(\S*)')

# The top level handler. This essentially calls out to the specific
# handler function depending on the uri that we're handling
def handler(req):
    # Set the no_cache flag on all our output
    # no_cache is not writable, have to set the headers directly
    req.headers_out['Cache-Control'] = 'no-cache'
    req.headers_out['Expired'] = '-1'

    # Parse the uri we were given.
    # This gives everything from the uri below the handler
    # eg if we're handling /python and we're the client requests
    # http://server/python/a/b.fits then we get a/b.fits
    uri = req.uri
    
    # Split this about any /s
    things = uri.split('/')

    # Remove any blanks
    while(things.count('')):
        things.remove('')

    # Check if it's empty
    if(len(things) == 0):
        # Empty request
        if(use_as_archive):
            util.redirect(req, "/searchform")
        else:
            return usagemessage(req)

    # Before we process the request, parse any arguments into a list
    args = []
    if(req.args):
        args = req.args.split('&')
        while(args.count('')):
            args.remove('')
 
    # OK, need to parse what we got.

    this = things.pop(0)

    # Archive searchform
    if(this == 'searchform'):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return searchform(req, things)

    # A debug util
    if(this == 'debug'):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return debugmessage(req)

    # This is the header summary handler
    if(this in ['summary', 'diskfiles', 'ssummary', 'lsummary']):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN

        links = True
        # the nolinks thing is for the external email notifications
        if 'nolinks' in things:
            links = False
            things.remove('nolinks')

        download = False
        # this says whether to include [download] links after the filename
        if 'download' in things:
            download = True
            things.remove('download')

        # Parse the rest of the uri here while we're at it
        # Expect some combination of program_id, observation_id, date and instrument name
        # We put the ones we got in a dictionary
        selection = getselection(things)

        # We should parse the arguments here too
        # All we have for now are order_by arguments
        # We form a list of order_by keywords
        # We should probably do more validation here
        orderby = []
        for i in range(len(args)):
            match = orderbycre.match(args[i])
            if(match):
                orderby.append(match.group(1))

        retval = summary(req, this, selection, orderby, links, download)
        return retval

    # This is the standard star in observation server
    if(this == 'standardobs'):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        header_id = things.pop(0)
        retval = standardobs(req, header_id)
        return retval


    # The calibrations handler
    if(this == 'calibrations'):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        # Parse the rest of the URL.
        selection = getselection(things)

        # If we want other arguments like order by
        # we should parse them here

        retval = calibrations(req, selection)
        return retval

    # The xml and json file list handlers
    if(this == 'xmlfilelist'):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        selection = getselection(things)
        retval = xmlfilelist(req, selection)
        return retval
    if(this == 'jsonfilelist'):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        selection = getselection(things)
        retval = jsonfilelist(req, selection)
        return retval

    # The fileontape handler
    if(this == 'fileontape'):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        retval = fileontape(req, things)
        return retval

    # The calmgr handler
    if(this == 'calmgr'):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        # Parse the rest of the URL.
        selection = getselection(things)

        # If we want other arguments like order by
        # we should parse them here

        retval = calmgr(req, selection)
        return retval

    # The processed_cal upload server
    if(this == 'upload_processed_cal'):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        retval = upload_file(req, things[0], processed_cal=True)
        return retval

    # The generic uploaD_file server
    if(this == 'upload_file'):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        retval = upload_file(req, things[0])
        return retval

    # This returns the fitsverify, wmdreport or fullheader text from the database
    # you can give it either a diskfile_id or a filename
    if(this == 'fitsverify' or this == 'wmdreport' or this == 'fullheader'):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        if(len(things) == 0):
            req.content_type = "text/plain"
            req.write("You must specify a filename or diskfile_id, eg: /fitsverify/N20091020S1234.fits\n")
            return apache.OK
        thing = things.pop(0)

        # OK, see if we got a filename
        fnthing = gemini_fitsfilename(thing)
        if(fnthing):
            # Now construct the query
            session = sessionfactory()
            try:
                query = session.query(File).filter(File.name == fnthing)
                if(query.count() == 0):
                    req.content_type = "text/plain"
                    req.write("Cannot find file for: %s\n" % fnthing)
                    return apache.OK
                file = query.one()
                # Query diskfiles to find the diskfile for file that is canonical
                query = session.query(DiskFile).filter(DiskFile.canonical == True).filter(DiskFile.file_id == file.id)
                diskfile = query.one()
                # Find the diskfilereport
                query = session.query(DiskFileReport).filter(DiskFileReport.diskfile_id == diskfile.id)
                diskfilereport = query.one()
                req.content_type = "text/plain"
                if(this == 'fitsverify'):
                    req.write(diskfilereport.fvreport)
                if(this == 'wmdreport'):
                    req.write(diskfilereport.wmdreport)
                if(this == 'fullheader'):
                    # Need to find the header associated with this diskfile
                    query = session.query(FullTextHeader).filter(FullTextHeader.diskfile_id == diskfile.id)
                    ftheader = query.one()
                    req.write(ftheader.fulltext)
                return apache.OK
            except IOError:
                pass
            finally:
                session.close()
     
        # See if we got a diskfile_id
        match = re.match('\d+', thing)
        if(match):
            session = sessionfactory()
            try:
                query = session.query(DiskFile).filter(DiskFile.id == thing)
                if(query.count() == 0):
                    req.content_type = "text/plain"
                    req.write("Cannot find diskfile for id: %s\n" % thing)
                    session.close()
                    return apache.OK
                diskfile = query.one()
                # Find the diskfilereport
                query = session.query(DiskFileReport).filter(DiskFileReport.diskfile_id == diskfile.id)
                diskfilereport = query.one()
                req.content_type = "text/plain"
                if(this == 'fitsverify'):
                    req.write(diskfilereport.fvreport)
                if(this == 'wmdreport'):
                    req.write(diskfilereport.wmdreport)
                if(this == 'fullheader'):
                    # Need to find the header associated with this diskfile
                    query = session.query(FullTextHeader).filter(FullTextHeader.diskfile_id == diskfile.id)
                    ftheader = query.one()
                    req.write(ftheader.fulltext)
                return apache.OK
            except IOError:
                pass
            finally:
                session.close()

        # OK, they must have fed us garbage
        req.content_type = "text/plain"
        req.write("Could not understand argument - You must specify a filename or diskfile_id, eg: /fitsverify/N20091020S1234.fits\n")
        return apache.OK

    # This is the fits file server
    if(this == 'file'):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return fileserver(req, things)

    # This is the projects observed feature
    if(this == "programsobserved"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        selection = getselection(things)
        if(("date" not in selection) and ("daterange" not in selection)):
            selection["date"] = gemini_date("today")
        retval = progsobserved(req, selection)
        return retval
        
    # The GMOS twilight flat and bias report
    if(this == "gmoscal"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        selection = getselection(things)
        retval = gmoscal(req, selection)
        return retval

    # Submit QA metric measurement report
    if(this == "qareport"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return qareport(req)

    # Retrieve QA metrics, simple initial version
    if(this == "qametrics"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return qametrics(req, things)

    # Retrieve QA metrics, json version for GUI
    if(this == "qaforgui"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return qaforgui(req, things)

    # Database Statistics
    if(this == "content"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return content(req)

    if(this == "stats"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return stats(req)

    # Tape handler
    if(this == "tape"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return tape(req, things)

    # TapeWrite handler
    if(this == "tapewrite"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return tapewrite(req, things)

    # TapeFile handler
    if(this == "tapefile"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return tapefile(req, things)

    # TapeRead handler
    if(this == "taperead"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return taperead(req, things)

    # XML Tape handler
    if(this == "xmltape"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return xmltape(req)

    # Emailnotification handler
    if(this == "notification"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return notification(req, things)

    # curation_report handler
    if(this == "curation"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return curation_report(req, things)

    # new account request
    if(this == "request_account"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return request_account(req, things)

    # account password reset request
    if(this == "password_reset"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return password_reset(req, things)

    # request password reset email
    if(this == "request_password_reset"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return request_password_reset(req)

    # login form
    if(this == "login"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return login(req, things)

    # logout
    if(this == "logout"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return logout(req)

    # whoami
    if(this == "whoami"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return whoami(req, things)

    # change_password
    if(this == "change_password"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return change_password(req, things)

    # my_programs
    if(this == "my_programs"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return my_programs(req, things)

    # staff_access
    if(this == "staff_access"):
        if(this in blocked_urls):
            return apache.HTTP_FORBIDDEN
        return staff_access(req, things)


    # Some static files that the server should serve via a redirect.
    staticfiles = ["robots.txt", "favicon.ico", "jquery-1.11.1.min.js"]
    if this in staticfiles:
        newurl = "/htmldocs/%s" % this
        util.redirect(req, newurl)
    
    # Last one on the list - if we haven't return(ed) out of this function
    # by one of the methods above, then we should send out the usage message
    return usagemessage(req)

# End of apache handler() function.
# Below are various helper functions called from above.
# The web summary has it's own package

# Send usage message to browser
def usagemessage(req):
    fp = open("/opt/FitsStorage/htmldocroot/htmldocs/usage.html", "r")
    stuff = fp.read()
    fp.close()
    req.content_type = "text/html"
    req.write(stuff)
    return apache.OK

# Send debugging info to browser
def debugmessage(req):
    req.content_type = "text/plain"
    req.write("Debug info\n\n")
    req.write("python interpreter name: %s\n\n" % (str(req.interpreter)))
    req.write("Pythonpath: %s\n\n" % (str(sys.path)))
    req.write("python path: \n")
    for i in sys.path:
      req.write("-- %s\n" % i)
    req.write("\n")
    req.write("uri: %s\n\n" % (str(req.uri)))
    req.write("unparsed_uri: %s\n\n" % (str(req.unparsed_uri)))
    req.write("the_request: %s\n\n" % (str(req.the_request)))
    req.write("filename: %s\n\n" % (str(req.filename)))
    req.write("path_info: %s\n\n" % (str(req.path_info)))
    req.write("args: %s\n\n" % (str(req.args)))
    
    return apache.OK
