# Provides functionality to extract and present full header information or metadata reports

import re

from gemini_metadata_utils import gemini_fitsfilename
from mod_python import apache

from orm import sessionfactory
from orm.file import File
from orm.diskfile import DiskFile
from orm.diskfilereport import DiskFileReport
from orm.fulltextheader import FullTextHeader

def report(req, thing):
    this = req.usagelog.this


#    if not (fnthing or match):
#        # OK, they must have fed us garbage
#        req.content_type = "text/plain"
#        req.write("Could not understand argument - You must specify a filename or diskfile_id, eg: /fitsverify/N20091020S1234.fits\n")
#
#        return apache.HTTP_OK

    session = sessionfactory()

    try:
        if thing.isdigit():
            # We got a diskfile_id
            query = session.query(DiskFile).filter(DiskFile.id == thing)
            if query.count() == 0:
                req.content_type = "text/plain"
                req.write("Cannot find diskfile for id: %s\n" % thing)
                return apache.OK
        # Now construct the query
        else:
            fnthing = gemini_fitsfilename(thing)
            # We got a filename
            if fnthing:
                error_message = "Cannot find file for: %s\n" % fnthing
                query = session.query(File).filter(File.name == fnthing)
            else:
                error_message = "Cannot find (non-standard named) file for: %s\n" % thing
                query = session.query(File).filter(File.name == thing)

            if query.count() == 0:
                req.content_type = "text/plain"
                req.write("Cannot find file for: %s\n" % fnthing)
                return apache.HTTP_OK
            file = query.one()
            # Query diskfiles to find the diskfile for file that is canonical
            query = session.query(DiskFile).filter(DiskFile.canonical == True).filter(DiskFile.file_id == file.id)
        else:
            # We got a diskfile_id
            query = session.query(DiskFile).filter(DiskFile.id == thing)
            if query.count() == 0:
                req.content_type = "text/plain"
                req.write("Cannot find diskfile for id: %s\n" % thing)
                return apache.HTTP_OK

        diskfile = query.one()
        # Find the diskfilereport
        query = session.query(DiskFileReport).filter(DiskFileReport.diskfile_id == diskfile.id)
        diskfilereport = query.one()
        req.content_type = "text/plain"
        if this == 'fitsverify':
            req.write(diskfilereport.fvreport)
        if this == 'mdreport':
            try:
                req.write(diskfilereport.mdreport)
            except TypeError:
                req.write('No report was generated\n')
        if this == 'fullheader':
            # Need to find the header associated with this diskfile
            query = session.query(FullTextHeader).filter(FullTextHeader.diskfile_id == diskfile.id)
            ftheader = query.one()
            req.write(ftheader.fulltext)
    except IOError:
        pass
    finally:
        session.close()

    return apache.HTTP_OK
