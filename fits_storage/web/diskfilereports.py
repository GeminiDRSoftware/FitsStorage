# Provides functionality to extract and present full header information or
# metadata reports

import re

from sqlalchemy.exc import NoResultFound

from fits_storage.gemini_metadata_utils import gemini_fitsfilename

from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.diskfilereport import DiskFileReport
from fits_storage.core.orm.header import Header
from fits_storage.core.orm.fulltextheader import FullTextHeader

from fits_storage.server.access_control_utils import canhave_coords

from fits_storage.server.wsgi.context import get_context
from fits_storage.server.wsgi.returnobj import Return

def report(thing):
    ctx = get_context()
    resp = ctx.resp
    session = ctx.session

    if thing is None:
        # OK, they must have fed us garbage
        resp.content_type = "text/plain"
        resp.append("Could not understand argument - You must specify a filename or diskfile_id, eg: /fitsverify/N20091020S1234.fits\n")

        return

    # TODO - this is a horrible hack, not a neat solution.
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
    diskfilereport = query.one_or_none()
    resp.content_type = "text/plain"

    if diskfilereport is None:
        resp.append('Cannot find report for: %s\n' % diskfile.filename)
    else:
        if this == 'fitsverify':
            if diskfilereport.fvreport is None:
                resp.append("No fitsverify report found for file\n")
            else:
                resp.append(diskfilereport.fvreport)
        elif this == 'mdreport':
            try:
                if diskfilereport.mdreport is None:
                    resp.append("No Metadata report found for file\n")
                else:
                    resp.append(diskfilereport.mdreport)
            except TypeError:
                resp.append('No report was generated\n')
        elif this == 'fullheader':
            # Need to find the header associated with this diskfile
            query = (session.query(Header, FullTextHeader)
                     .filter(FullTextHeader.diskfile_id == diskfile.id)
                     .filter(Header.diskfile_id == FullTextHeader.diskfile_id))  # explicit join to keep SQLA happy
            try:
                header, ftheader = query.one()
                if canhave_coords(session, ctx.user, header):
                    resp.append(ftheader.fulltext)
                else:
                    resp.client_error(Return.HTTP_FORBIDDEN, "The data you're trying to access has "
                                                             "proprietary rights and cannot be displayed")
            except NoResultFound:
                resp.append("No stored header for file.\n")

            if diskfile.provenance:
                resp.append("\n\n")
                resp.append("------ PROVENANCE ------\n")
                filename_length = len('Filename')
                md5_length = len('MD5')
                addedby_length = len('Added By')
                for provenance in diskfile.provenance:
                    filename_length = max(filename_length, len(provenance.filename))
                    md5_length = max(md5_length, len(provenance.md5))
                    addedby_length = max(addedby_length, len(provenance.added_by))
                resp.append("%s %s %s %s\n" % ('Filename'.ljust(filename_length),
                                               'MD5'.ljust(md5_length),
                                               'Timestamp'.ljust(26),
                                               'Added By'.ljust(addedby_length)))
                for provenance in diskfile.provenance:
                    resp.append("%s %s %s %s\n" % (provenance.filename.ljust(filename_length),
                                                   provenance.md5.ljust(md5_length),
                                                   provenance.timestamp,
                                                   provenance.added_by.ljust(addedby_length)))
            if diskfile.history:
                resp.append("\n\n")
                resp.append("------ HISTORY ------\n")
                primitive_length = len('Primitive')
                for history in diskfile.history:
                    primitive_length = max(primitive_length, len(history.primitive))
                resp.append("%s %s %s %s\n" % ('Start '.ljust(26),
                                               'End'.ljust(26),
                                               'Primitive'.ljust(primitive_length),
                                               'Args'))
                for history in diskfile.history:
                    resp.append("%s %s %s %s\n" % (
                        history.timestamp_start,
                        history.timestamp_end,
                        history.primitive.ljust(primitive_length),
                        history.args))
