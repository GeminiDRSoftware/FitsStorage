# Provides functionality to extract and present full header information or metadata reports

import re

from ..gemini_metadata_utils import gemini_fitsfilename
from mod_python import apache

from ..orm import session_scope
from ..orm.file import File
from ..orm.diskfile import DiskFile
from ..orm.diskfilereport import DiskFileReport
from ..orm.header import Header
from ..orm.fulltextheader import FullTextHeader

from ..utils.userprogram import canhave_coords
from ..utils.web import Context

def report(req, thing):
    ctx = Context()
    resp = ctx.resp
    this = ctx.usagelog.this

#    if not (fnthing or match):
#        # OK, they must have fed us garbage
#        req.content_type = "text/plain"
#        req.write("Could not understand argument - You must specify a filename or diskfile_id, eg: /fitsverify/N20091020S1234.fits\n")
#
#        return apache.HTTP_OK

    with session_scope() as session:
        if thing.isdigit():
            # We got a diskfile_id
            query = session.query(DiskFile).filter(DiskFile.id == thing)
            if query.count() == 0:
                resp.content_type = "text/plain"
                resp.append("Cannot find diskfile for id: %s\n" % thing)
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
                resp.content_type = "text/plain"
                resp.append(error_message)
                return apache.HTTP_OK
            file = query.one()
            # Query diskfiles to find the diskfile for file that is canonical
            query = session.query(DiskFile).filter(DiskFile.canonical == True).filter(DiskFile.file_id == file.id)

        diskfile = query.one()
        # Find the diskfilereport
        query = session.query(DiskFileReport).filter(DiskFileReport.diskfile_id == diskfile.id)
        diskfilereport = query.one()
        resp.content_type = "text/plain"
        if this == 'fitsverify':
            resp.append(diskfilereport.fvreport)
        if this == 'mdreport':
            try:
                resp.append(diskfilereport.mdreport)
            except TypeError:
                resp.append('No report was generated\n')
        if this == 'fullheader':
            # Need to find the header associated with this diskfile
            query = (session.query(Header, FullTextHeader)
                        .filter(FullTextHeader.diskfile_id == diskfile.id)
                        .filter(Header.diskfile_id == diskfile.id))
            header, ftheader = query.one()
            if canhave_coords(session, ctx.user, header):
                resp.append(ftheader.fulltext)
            else:
                resp.append("The data you're trying to access has proprietary rights and cannot be displayed")

    return apache.HTTP_OK
