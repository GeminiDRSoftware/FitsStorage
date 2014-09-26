"""
This module handles the web 'logreports' functions - presenting data from the usage, query, upload and download logs
"""
import datetime
import dateutil.parser

from sqlalchemy import desc

from orm import sessionfactory
from orm.usagelog import UsageLog
from orm.querylog import QueryLog
from orm.downloadlog import DownloadLog
from orm.user import User

from web.user import userfromcookie

from mod_python import apache
from mod_python import util

def usagereport(req):
    """
    This is the usage report handler
    """

    session = sessionfactory()
    try:
        # Need to be logged in as gemini staff to do this
        user = userfromcookie(session, req)
        if user is None or user.gemini_staff is False:
            return apache.HTTP_FORBIDDEN

        # Process the form data if there is any
        # Default all the pre-fill strings
        start = ''
        end = ''
        username = ''
        ipaddr = ''
        this = ''
        status = ''

        formdata = util.FieldStorage(req)
        if len(formdata.keys()) > 0:
            # We got a form submitted, process values
            for key in formdata.keys():
                if key == 'start' and len(formdata[key]):
                    start = dateutil.parser.parse(formdata[key])
                elif key == 'end' and len(formdata[key]):
                    end = dateutil.parser.parse(formdata[key])
                elif key == 'username' and len(formdata[key]):
                    user = session.query(User).filter(User.username == formdata[key]).first()
                    if user:
                        username = user.username
                        user_id = user.id
                elif key == 'ipaddr' and len(formdata[key]):
                    ipaddr = str(formdata[key])
                elif key == 'this' and len(formdata[key]):
                    this = str(formdata[key])
                elif key == 'status' and len(formdata[key]):
                    try:
                        status = int(formdata[key])
                    except:
                        pass
                    
        # send them the form, pre-populate if have values
        req.content_type = "text/html"

        req.write('<!DOCTYPE html><html><head>')
        req.write('<meta charset="UTF-8">')
        req.write('<link rel="stylesheet" type="text/css" href="/htmldocs/table.css">')
        req.write('<title>Fits Server Log Query</title></head>')
        req.write("<body><h1>Fits Server Log Query</h1>")
        req.write("<FORM action='/usagereport' method='POST'>")
        req.write("<TABLE>")
        # Start and End of Query
        req.write('<TR><TD><LABEL for="start">UT Start of Report</LABEL></TD>')
        req.write('<TD><INPUT type="text" size=20 name="start" value=%s></TD></TR>' % start)
        req.write('<TR><TD><LABEL for="end">UT End of Report</LABEL></TD>')
        req.write('<TD><INPUT type="text" size=20 name="end" value=%s></TD></TR>' % end)
        
        req.write('<TR><TD><LABEL for="username">Username</LABEL></TD>')
        req.write('<TD><INPUT type="text" size=20 name="username" value=%s></TD></TR>' % username)

        req.write('<TR><TD><LABEL for="ipaddr">IP address</LABEL></TD>')
        req.write('<TD><INPUT type="text" size=20 name="ipaddr" value=%s></TD></TR>' % ipaddr)

        req.write('<TR><TD><LABEL for="this">"This" feature</LABEL></TD>')
        req.write('<TD><INPUT type="text" size=20 name="this" value=%s></TD></TR>' % this)

        req.write('<TR><TD><LABEL for="status">HTTP Status</LABEL></TD>')
        req.write('<TD><INPUT type="text" size=4 name="status" value=%s></TD></TR>' % status)

        req.write("</TABLE>")
        req.write('<INPUT type="submit" value="Submit"></INPUT>')
        req.write("</FORM>")

        if len(formdata.keys()) > 0:
            req.write("<h1>Usage Report</h1>")
            query = session.query(UsageLog)
            if start:
                query.filter(UsageLog.utdatetime >= start)
            if end:
                query.filter(UsageLog.utdatetime <= end)
            if username:
                user = session.query(User).filter(User.username == username).first()
                if user:
                    query = query.filter(UsageLog.user_id == user.id)
            if ipaddr:
                query = query.filter(UsageLog.ip_address == ipaddr)
            if this:
                query = query.filter(UsageLog.this == this)
            if status:
                try:
                    query = query.filter(UsageLog.status == int(status))
                except:
                    pass

            query = query.order_by(desc(UsageLog.utdatetime))
            usagelogs = query.all()

            req.write('<TABLE>')
            req.write('<TR class="tr_head">')
            req.write('<TH colspan=9>Usage</TH>')
            req.write('<TH colspan=6>Query</TH>')
            req.write('<TH colspan=7>Download</TH>')
            req.write('</TR>\n')
            req.write('<TR class="tr_head">')
            req.write('<TH>ID</TH>')
            req.write('<TH>UT Date Time</TH>')
            req.write('<TH>Username</TH>')
            req.write('<TH>IP Address</TH>')
            req.write('<TH>HTTP</TH>')
            req.write('<TH>This</TH>')
            req.write('<TH>Bytes</TH>')
            req.write('<TH>Status</TH>')
            req.write('<TH>Notes</TH>')
            # Query part
            req.write('<TH>N res</TH>')
            req.write('<TH>N cal</TH>')
            req.write('<TH>T res</TH>')
            req.write('<TH>T cal</TH>')
            req.write('<TH>T sum</TH>')
            req.write('<TH>Notes</TH>')
            # Download part
            req.write('<TH>N res</TH>')
            req.write('<TH>N den</TH>')
            req.write('<TH>Send</TH>')
            req.write('<TH>T res</TH>')
            req.write('<TH>T DL</TH>')
            req.write('<TH>MB/sec</TH>')
            req.write('<TH>Notes</TH>')
            req.write('</TR>\n')
 
            even = False
            for usagelog in usagelogs:

                even = not even
                if even:
                    tr_class = "tr_even"
                else:
                    tr_class = "tr_odd"

                req.write('<TR class="%s">' % tr_class)
                req.write('<TD>%d</TD>' % usagelog.id)
                req.write('<TD>%s</TD>' % usagelog.utdatetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:21])
                html = ""
                if usagelog.user_id:
                    user = session.query(User).filter(User.id == usagelog.user_id).one()
                    html = user.username
                    if user.gemini_staff:
                        html += " (Staff)"
                req.write('<TD>%s</TD>' % html)
                req.write('<TD>%s</TD>' % usagelog.ip_address)
                req.write('<TD>%s</TD>' % usagelog.method)
                req.write('<TD>%s</TD>' % usagelog.this)
                req.write('<TD>%s</TD>' % usagelog.bytes)
                html = str(usagelog.status)
                if usagelog.status == 200:
                    html += " (OK)"
                if usagelog.status == 403:
                    html += " (FORBIDDEN)"
                if usagelog.status == 500:
                    html += " (SERVER ERROR)"
                if usagelog.status == 404:
                    html += " (NOT FOUND)"
                req.write('<TD>%s</TD>' % html)
                req.write('<TD>%s</TD>' % usagelog.notes)

                # Query part
                querylog = session.query(QueryLog).filter(QueryLog.usagelog_id == usagelog.id).first()
                if querylog:
                    req.write('<TD>%s</TD>' % querylog.numresults)

                    html = ''
                    if querylog.numcalresults:
                        html = str(querylog.numcalresults)
                    req.write('<TD>%s</TD>' % html)

                    html = ''
                    if querylog.query_completed and querylog.query_started:
                        tres = querylog.query_completed - querylog.query_started
                        html = '%.2f' % tres.total_seconds()
                    req.write('<TD>%s</TD>' % html)

                    html = ''
                    if querylog.cals_completed and querylog.query_started:
                        tcal = querylog.cals_completed - querylog.query_started
                        html = '%.2f' % tcal.total_seconds()
                    req.write('<TD>%s</TD>' % html)

                    html = ''
                    if querylog.summary_completed and querylog.query_started:
                        tsum = querylog.summary_completed - querylog.query_started
                        html = '%.2f' % tsum.total_seconds()
                    req.write('<TD>%s</TD>' % html)

                    html = '<abbr title="%s">Sel</abbr>' % querylog.selection
                    if querylog.notes:
                        html += ' <abbr title="%s">Notes</abbr>' % querylog.notes
                    req.write('<TD>%s</TD>' % html)
                else:
                    req.write('<TD colspan=6></TD>')

                # Download part
                dllog = session.query(DownloadLog).filter(DownloadLog.usagelog_id == usagelog.id).first()
                if dllog:
                    req.write('<TD>%s</TD>' % dllog.numresults)

                    if dllog.numdenied:
                        req.write('<TD>%s</TD>' % dllog.numdenied)
                    else:
                        req.write('<TD></TD>')


                    req.write('<TD>%s</TD>' % dllog.sending_files)

                    html = ''
                    if dllog.query_started and dllog.query_completed:
                        tres = dllog.query_completed - dllog.query_started
                        html = '%.2f' % tres.total_seconds()
                    req.write('<TD>%s</TD>' % html)

                    html = ''
                    if dllog.query_completed and dllog.download_completed:
                        tdl = dllog.download_completed - dllog.query_completed
                        tdlsecs = tdl.total_seconds()
                        html = '%.2f' % tdlsecs
                        bytes = usagelog.bytes
                        bytespersec = bytes / tdlsecs
                    req.write('<TD>%s</TD><TD>%.2f</TD>' % (html, bytespersec / 1000000.0))

                    html = '<abbr title="%s">Sel</abbr>' % dllog.selection
                    if dllog.notes:
                        html += ' <abbr title="%s">Notes</abbr>' % dllog.notes
                    req.write('<TD>%s</TD>' % html)

                else:
                    req.write('<TD colspan=7></TD>')

                req.write('</TR>\n')
            req.write('</TABLE>')
        req.write("</body></html>")

    finally:
        session.close()

    return apache.OK
