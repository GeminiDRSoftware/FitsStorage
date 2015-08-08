"""
This is the Fits Storage Web Summary module. It provides the functions
which query the database and generate html for the web header
summaries.
"""
from ..orm import sessionfactory
from mod_python import apache

from ..orm.photstandard import PhotStandard, PhotStandardObs
from ..orm.footprint import Footprint

def list_phot_std_obs(session, header_id):
    """
    Returns a list of the photometric standards that should be in this header id
    """

    query = session.query(PhotStandardObs.photstandard_id).select_from(PhotStandardObs, Footprint)
    query = query.filter(PhotStandardObs.footprint_id == Footprint.id)
    query = query.filter(Footprint.header_id == header_id)

    for q in query:
        yield q

bands = ('u', 'v', 'g', 'r', 'i', 'z', 'y', 'j', 'h', 'k', 'lprime', 'm')

def standardobs(req, header_id):
    """
    sends and html table detailing the standard stars visisble in this header_id
    """

    session = sessionfactory()
    try:
        req.content_type = "text/html"
        req.write("<html>")
        title = "Photometric standards in frame"
        req.write("<head>")
        req.write("<title>%s</title>" % (title))
        req.write('<link rel="stylesheet" href="/table.css">')
        req.write("</head>\n")
        req.write("<body>")
        req.write("<H1>%s</H1>" % (title))

        req.write('<TABLE border=0>')

        req.write('<TR class=tr_head>')
        req.write('<TH>Name</TH>')
        req.write('<TH>Field</TH>')
        req.write('<TH>RA</TH>')
        req.write('<TH>Dec</TH>')
        for band in bands:
            req.write('<TH>%s_mag</TH>' % (band if band != 'lprime' else 'l_prime'))

        lst = list_phot_std_obs(session, header_id)
        even = False
        for std_id in list_phot_std_obs(session, header_id):
            std = session.query(PhotStandard).filter(PhotStandard.id == std_id).one()
            even = not even
            req.write("<TR class=%s>" % ('tr_even' if even else 'tr_odd'))
            req.write("<TD>%s</TD>" % std.name)
            req.write("<TD>%s</TD>" % std.field)
            req.write("<TD>%f</TD>" % std.ra)
            req.write("<TD>%f</TD>" % std.dec)
            for band in bands:
                try:
                    req.write("<TD>%f</TD>" % getattr(std, band + '_mag'))
                except TypeError:
                    req.write("<TD></TD>")
            req.write("</TR>")

        req.write("</TABLE>")
        req.write("</body></html>")

        return apache.HTTP_OK

    finally:
        session.close()

def xmlstandardobs(req, header_id):
    """
    Writes xml fragment defining the standards visible in this header_id
    """

    session = sessionfactory()
    try:
        lst = list_phot_std_obs(session, header_id)
        for std_id in lst:
            std = session.query(PhotStandard).filter(PhotStandard.id == std_id).one()
            req.write("<photstandard>")
            req.write("<name>%s</name>" % std.name)
            req.write("<field>%s</field>" % std.field)
            req.write("<ra>%f</ra>" % std.ra)
            req.write("<dec>%f</dec>" % std.dec)
            for bandmag in (band + '_mag' for band in bands):
                value = getattr(std, bandmag)
                if value:
                    req.write("<%(bm)s>%(value)f</%(bm)s>" % {'bm': bandmag, 'value': value})
            req.write("</photstandard>")


    finally:
        session.close()

