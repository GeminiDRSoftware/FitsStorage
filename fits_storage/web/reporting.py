# Provides functionality to extract and present full header information or metadata reports

import re

from ..gemini_metadata_utils import gemini_fitsfilename

from ..orm.file import File
from ..orm.diskfile import DiskFile
from ..orm.diskfilereport import DiskFileReport
from ..orm.header import Header
from ..orm.fulltextheader import FullTextHeader

from ..utils.userprogram import canhave_coords
from ..utils.web import get_context, Return

def report(thing):
    ctx = get_context()
    resp = ctx.resp
    session = ctx.session

    if thing is None:
        # OK, they must have fed us garbage
        resp.content_type = "text/plain"
        resp.append("Could not understand argument - You must specify a filename or diskfile_id, eg: /fitsverify/N20091020S1234.fits\n")

        return

    this = ctx.usagelog.this

    if thing.isdigit():
        # We got a diskfile_id
        query = session.query(DiskFile).filter(DiskFile.id == thing)
        if query.count() == 0:
            resp.content_type = "text/plain"
            resp.append("Cannot find diskfile for id: %s\n" % thing)
            return
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
            resp.client_error(Return.HTTP_NOT_FOUND, error_message)
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
    elif this == 'mdreport':
        try:
            resp.append(diskfilereport.mdreport)
        except TypeError:
            resp.append('No report was generated\n')
    elif this == 'fullheader':
        # Need to find the header associated with this diskfile
        query = (session.query(Header, FullTextHeader)
                    .filter(FullTextHeader.diskfile_id == diskfile.id)
                    .filter(Header.diskfile_id == diskfile.id))
        header, ftheader = query.one()
        if canhave_coords(session, ctx.user, header):
            resp.append(ftheader.fulltext)
        else:
            resp.client_error(Return.HTTP_FORBIDDEN, "The data you're trying to access has "
                                                     "proprietary rights and cannot be displayed")

        if hasattr(diskfile, 'PROVENANCE'):
            resp.append("\n\n")
            resp.append("------ PROVENANCE ------\n")
            filename_length = len('Filename')
            md5_length = len('MD5')
            primitive_length = len('Primitive')
            for provenance in diskfile.PROVENANCE:
                filename_length = max(filename_length, len(provenance[1]))
                md5_length = max(md5_length, len(provenance[2]))
                primitive_length = max(primitive_length, len(provenance[3]))
            resp.append("%s %s %s %s\n" % ('Filename'.ljust(filename_length),
                                           'MD5'.ljust(md5_length),
                                           'Timestamp'.ljust(26),
                                           'Provenance Added By'.ljust(primitive_length)))
            for provenance in diskfile.PROVENANCE:
                resp.append("%s %s %s %s\n" % (provenance[1].ljust(filename_length),
                                               provenance[2].ljust(md5_length),
                                               provenance[0],
                                               provenance[3].ljust(primitive_length)))
        if hasattr(diskfile, 'PROVENANCE_HISTORY'):
            resp.append("\n\n")
            resp.append("------ HISTORY ------\n")
            for phistory in diskfile.PROVENANCE_HISTORY:
                resp.append("start:     %s\nend:       %s\nprimitive: %s\nargs:      %s\n\n"
                            % (phistory[0],
                               phistory[1],
                               phistory[2], 
                               phistory[3]))
