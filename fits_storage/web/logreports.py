"""
This module handles the web 'logreports' functions - presenting data from the usage, query, upload and download logs
"""
import datetime
import dateutil.parser
from collections import namedtuple

from sqlalchemy import and_, between, cast, desc, extract, func, join
from sqlalchemy import BigInteger, Date, Integer, Interval, String
from sqlalchemy.orm import aliased

from ..orm import sessionfactory
from ..orm.usagelog import UsageLog
from ..orm.querylog import QueryLog
from ..orm.downloadlog import DownloadLog
from ..orm.filedownloadlog import FileDownloadLog
from ..orm.fileuploadlog import FileUploadLog
from ..orm.user import User

from ..gemini_metadata_utils import ONEDAY_OFFSET

from .user import userfromcookie
from .selection import getselection, queryselection, sayselection
from .list_headers import list_headers

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
        # Default to last day
        today = datetime.datetime.utcnow().date()
        tomorrow = today + ONEDAY_OFFSET
        start = today.isoformat()
        end = tomorrow.isoformat()
        username = ''
        ipaddr = ''
        this = ''
        status = ''

        formdata = util.FieldStorage(req)
        for key, value in formdata.items():
            if key == 'start' and len(value):
                start = dateutil.parser.parse(value)
            elif key == 'end' and len(value):
                end = dateutil.parser.parse(value)
            elif key == 'username' and len(value):
                user = session.query(User).filter(User.username == formdata[key]).first()
                if user:
                    username = user.username
                    user_id = user.id
            elif key == 'ipaddr' and len(value):
                ipaddr = str(value)
            elif key == 'this' and len(value):
                this = str(value)
            elif key == 'status' and len(value):
                try:
                    status = int(value)
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
                query = query.filter(UsageLog.utdatetime >= start)
            if end:
                query = query.filter(UsageLog.utdatetime <= end)
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

            usagelogs = query.order_by(desc(UsageLog.utdatetime))

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

                req.write('<TR class="%s">' % ('tr_even' if even else 'tr_odd'))
                req.write('<TD><a href="/usagedetails/%d">%d</a></TD>' % (usagelog.id, usagelog.id))
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
                req.write('<TD>%s</TD>' % usagelog.status_string())
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
                    bytespersec = 0
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

    return apache.HTTP_OK

def usagedetails(req, things):
    """
    This is the usage report detail handler
    things should contain an useagelog ID number
    """

    session = sessionfactory()
    try:
        # Need to be logged in as gemini staff to do this
        user = userfromcookie(session, req)
        if user is None or user.gemini_staff is False:
            return apache.HTTP_FORBIDDEN


        if len(things) != 1:
            return apache.HTTP_NOT_ACCEPTABLE
        try:
            id = int(things[0])
        except:
            return apache.HTTP_NOT_ACCEPTABLE

        usagelog = session.query(UsageLog).filter(UsageLog.id == id).first()
        req.content_type = "text/html"
        req.write('<!DOCTYPE html><html><head>')
        req.write('<meta charset="UTF-8">')
        req.write('<link rel="stylesheet" type="text/css" href="/htmldocs/table.css">')
        req.write('<title>Fits Server Usage Log Detail</title></head>')
        req.write("<body><h1>Fits Server Usage Log Detail</h1>")
        req.write("<h2>Usage Log Entry</h2>")
        req.write("<TABLE>")
        req.write("<TR><TD>ID:</TD><TD>%s</TD>" % usagelog.id)
        req.write("<TR><TD>UT DateTime:</TD><TD>%s</TD></TR>" % usagelog.utdatetime)
        req.write("<TR><TD>User ID:</TD><TD>%s</TD></TR>" % usagelog.user_id)
        req.write("<TR><TD>IP address:</TD><TD>%s</TD></TR>" % usagelog.ip_address)
        req.write("<TR><TD>User Agent:</TD><TD>%s</TD></TR>" % usagelog.user_agent)
        req.write("<TR><TD>Referer:</TD><TD>%s</TD></TR>" % usagelog.referer)
        req.write("<TR><TD>HTTP method:</TD><TD>%s</TD></TR>" % usagelog.method)
        req.write("<TR><TD>URI:</TD><TD>%s</TD></TR>" % usagelog.uri)
        req.write("<TR><TD>This feature:</TD><TD>%s</TD></TR>" % usagelog.this)
        req.write("<TR><TD>Bytes returned:</TD><TD>%s</TD></TR>" % usagelog.bytes)
        req.write("<TR><TD>HTTP status:</TD><TD>%s</TD></TR>" % usagelog.status_string())
        req.write("<TR><TD>Notes:</TD><TD><PRE>%s</PRE></TD></TR>" % usagelog.notes)
        req.write("</TABLE>")

        if usagelog.user_id:
            user = session.query(User).filter(User.id == usagelog.user_id).one()
            req.write("<h2>User Details</h2>")
            req.write("<TABLE>")
            req.write("<TR><TD>Username:</TD><TD>%s</TD></TR>" % user.username)
            req.write("<TR><TD>Full Name:</TD><TD>%s</TD></TR>" % user.fullname)
            req.write("<TR><TD>Email:</TD><TD>%s</TD></TR>" % user.email)
            req.write("<TR><TD>Gemini Staff:</TD><TD>%s</TD></TR>" % user.gemini_staff)
            req.write("<TR><TD>Superuser:</TD><TD>%s</TD></TR>" % user.superuser)
            req.write("<TR><TD>Account Created:</TD><TD>%s</TD></TR>" % user.account_created)
            req.write("<TR><TD>Password Changed:</TD><TD>%s</TD></TR>" % user.password_changed)
            req.write("</TABLE>")

        querylog = session.query(QueryLog).filter(QueryLog.usagelog_id==usagelog.id).first()
        if querylog:
            req.write("<h2>Query Details</h2>")
            req.write("<TABLE>")
            req.write("<TR><TD>Summary Type:</TD><TD>%s</TD></TR>" % querylog.summarytype)
            req.write("<TR><TD>Selection:</TD><TD>%s</TD></TR>" % querylog.selection)
            req.write("<TR><TD>Number of Results:</TD><TD>%s</TD></TR>" % querylog.numresults)
            req.write("<TR><TD>Number of Calibration Results:</TD><TD>%s</TD></TR>" % querylog.numcalresults)
            req.write("<TR><TD>Query Started:</TD><TD>%s</TD></TR>" % querylog.query_started)
            req.write("<TR><TD>Query Completed:</TD><TD>%s</TD></TR>" % querylog.query_completed)
            req.write("<TR><TD>Cals Completed:</TD><TD>%s</TD></TR>" % querylog.cals_completed)
            req.write("<TR><TD>Summary Completed:</TD><TD>%s</TD></TR>" % querylog.summary_completed)
            if querylog.query_completed and querylog.query_started:
                td = querylog.query_completed - querylog.query_started
                req.write("<TR><TD>Query Seconds:</TD><TD>%.2f</TD></TR>" % td.total_seconds())
            if querylog.cals_completed and querylog.query_started:
                td = querylog.cals_completed - querylog.query_started
                req.write("<TR><TD>Cals query Seconds:</TD><TD>%.2f</TD></TR>" % td.total_seconds())
            if querylog.summary_completed and querylog.query_started:
                td = querylog.summary_completed - querylog.query_started
                req.write("<TR><TD>Summary Seconds:</TD><TD>%.2f</TD></TR>" % td.total_seconds())
            req.write("<TR><TD>Notes:</TD><TD>%s</TD></TR>" % querylog.notes)
            req.write("</TABLE>")

        downloadlog = session.query(DownloadLog).filter(DownloadLog.usagelog_id==usagelog.id).first()
        if downloadlog:
            req.write("<h2>Download Details</h2>")
            req.write("<TABLE>")
            req.write("<TR><TD>Selection:</TD><TD>%s</TD></TR>" % downloadlog.selection)
            req.write("<TR><TD>Num Results:</TD><TD>%s</TD></TR>" % downloadlog.numresults)
            req.write("<TR><TD>Sending Files:</TD><TD>%s</TD></TR>" % downloadlog.sending_files)
            req.write("<TR><TD>Num Denied:</TD><TD>%s</TD></TR>" % downloadlog.numdenied)
            req.write("<TR><TD>Query Started:</TD><TD>%s</TD></TR>" % downloadlog.query_started)
            req.write("<TR><TD>Query Completed:</TD><TD>%s</TD></TR>" % downloadlog.query_completed)
            req.write("<TR><TD>Download completed:</TD><TD>%s</TD></TR>" % downloadlog.download_completed)
            if downloadlog.query_completed and downloadlog.query_started:
                td = downloadlog.query_completed - downloadlog.query_started
                req.write("<TR><TD>Query Seconds:</TD><TD>%.2f</TD></TR>" % td.total_seconds())
            if downloadlog.download_completed and downloadlog.query_completed:
                td = downloadlog.download_completed - downloadlog.query_completed
                req.write("<TR><TD>Download Seconds:</TD><TD>%.2f</TD></TR>" % td.total_seconds())
                req.write("<TR><TD>Download Kbytes / sec:</TD><TD>%.2f</TD></TR>" % (usagelog.bytes / (1000*td.total_seconds())))
            req.write("<TR><TD>Notes:</TD><TD>%s</TD></TR>" % downloadlog.notes)
            req.write("</TABLE>")

        filedownloadlogs = session.query(FileDownloadLog).filter(FileDownloadLog.usagelog_id==usagelog.id).all()
        if len(filedownloadlogs):
            req.write("<h2>File Download Details</h2>")
            req.write("<TABLE>")
            req.write('<TR class="tr_head">')
            req.write("<TH>Filename</TH>")
            req.write("<TH>File size</TH>")
            req.write("<TH>File md5sum</TH>")
            req.write("<TH>UT DateTime</TH>")
            req.write("<TH>Released</TH>")
            req.write("<TH>PI Access</TH>")
            req.write("<TH>Staff Access</TH>")
            req.write("<TH>Magic Access</TH>")
            req.write("<TH>Eng Access</TH>")
            req.write("<TH>Can Have It</TH>")
            req.write("<TH>Notes</TH>")
            req.write("</TR>")
            even = False
            for filedownloadlog in filedownloadlogs:
                even = not even
                if even:
                    req.write('<TR class="tr_even">')
                else:
                    req.write('<TR class="tr_odd">')
                req.write("<TD>%s</TD>" % filedownloadlog.diskfile_filename)
                req.write("<TD>%d</TD>" % filedownloadlog.diskfile_file_size)
                req.write("<TD>%s</TD>" % filedownloadlog.diskfile_file_md5)
                req.write("<TD>%s</TD>" % filedownloadlog.ut_datetime)
                req.write("<TD>%s</TD>" % filedownloadlog.released)
                req.write("<TD>%s</TD>" % filedownloadlog.pi_access)
                req.write("<TD>%s</TD>" % filedownloadlog.staff_access)
                req.write("<TD>%s</TD>" % filedownloadlog.magic_access)
                req.write("<TD>%s</TD>" % filedownloadlog.eng_access)
                req.write("<TD>%s</TD>" % filedownloadlog.canhaveit)
                req.write("<TD>%s</TD>" % filedownloadlog.notes)
                req.write("</TR>")
            req.write("</TABLE>")

        fileuploadlogs = session.query(FileUploadLog).filter(FileUploadLog.usagelog_id==usagelog.id).all()
        if len(fileuploadlogs):
            req.write("<h2>File Upload Details</h2>")
            req.write("<TABLE>")
            req.write('<TR class="tr_head">')
            req.write("<TH>Transfer UT Start</TH>")
            req.write("<TH>Transfer UT Complete</TH>")
            req.write("<TH>Transfer Seconds</TH>")
            req.write("<TH>Transfer Kbyte/s </TH>")
            req.write("<TH>Filename</TH>")
            req.write("<TH>Size</TH>")
            req.write("<TH>MD5</TH>")
            req.write("<TH>Processed Cal</TH>")
            req.write("<TH>Invoke Status</TH>")
            req.write("<TH>Invoke PID</TH>")
            req.write("<TH>Destination</TH>")
            req.write("<TH>S3 UT Start</TH>")
            req.write("<TH>S3 UT End</TH>")
            req.write("<TH>S3 Seconds</TH>")
            req.write("<TH>S3 kbyte/s</TH>")
            req.write("<TH>S3 OK</TH>")
            req.write("<TH>File OK</TH>")
            req.write("<TH>IngestQueue ID</TH>")
            req.write("<TH>Notes</TH>")
            req.write("</TR>")
            even = False
            for fileuploadlog in fileuploadlogs:
                even = not even
                if even:
                    req.write('<TR class="tr_even">')
                else:
                    req.write('<TR class="tr_odd">')
                req.write("<TD>%s</TD>" % fileuploadlog.ut_transfer_start)
                req.write("<TD>%s</TD>" % fileuploadlog.ut_transfer_complete)
                if fileuploadlog.ut_transfer_start and fileuploadlog.ut_transfer_complete:
                    tdel = fileuploadlog.ut_transfer_complete - fileuploadlog.ut_transfer_start
                    req.write("<TD>%.2f</TD>" % tdel.total_seconds())
                    req.write("<TD>%.2f</TD>" % (fileuploadlog.size / (1000*tdel.total_seconds())))
                else:
                    req.write("<TD></TD><TD></TD>")
                req.write("<TD>%s</TD>" % fileuploadlog.filename)
                req.write("<TD>%s</TD>" % fileuploadlog.size)
                req.write("<TD>%s</TD>" % fileuploadlog.md5)
                req.write("<TD>%s</TD>" % fileuploadlog.processed_cal)
                req.write("<TD>%s</TD>" % fileuploadlog.invoke_status)
                req.write("<TD>%s</TD>" % fileuploadlog.invoke_pid)
                req.write("<TD>%s</TD>" % fileuploadlog.destination)
                req.write("<TD>%s</TD>" % fileuploadlog.s3_ut_start)
                req.write("<TD>%s</TD>" % fileuploadlog.s3_ut_end)
                if fileuploadlog.s3_ut_start and fileuploadlog.s3_ut_end:
                    tdel = fileuploadlog.s3_ut_end - fileuploadlog.s3_ut_start
                    req.write("<TD>%.2f</TD>" % tdel.total_seconds())
                    req.write("<TD>%.2f</TD>" % (fileuploadlog.size / (1000*tdel.total_seconds())))
                else:
                    req.write("<TD></TD><TD></TD>")
                req.write("<TD>%s</TD>" % fileuploadlog.s3_ok)
                req.write("<TD>%s</TD>" % fileuploadlog.file_ok)
                req.write("<TD>%s</TD>" % fileuploadlog.ingestqueue_id)
                req.write("<TD>%s</TD>" % fileuploadlog.notes)
                req.write("</TR>")
            req.write("<TABLE>")

        req.write("</body></html>")
    finally:
        session.close()

    return apache.HTTP_OK

def downloadlog(req, things):
    """
    This accepts a selection and returns a log showing all the downloads of the
    files that match the selection.
    """
    session = sessionfactory()

    try:
        # Need to be logged in as gemini staff to do this
        user = userfromcookie(session, req)
        if user is None or user.gemini_staff is False:
            return apache.HTTP_FORBIDDEN


        selection = getselection(things)

        req.content_type = "text/html"
        req.write('<!DOCTYPE html><html><head>')
        req.write('<meta charset="UTF-8">')
        req.write('<link rel="stylesheet" href="/htmldocs/table.css">')
        req.write("<title>Download Log</title>")
        req.write("</head>\n")
        req.write("<body>")
        req.write("<h1>File Download log</h1>")
        req.write("<h3>Selection: %s</h3>" % sayselection(selection))

        if 'notrecognised' in selection.keys():
            req.write("<H4>WARNING: I didn't recognize the following search terms: %s</H4>" % selection['notrecognised'])

        headers = list_headers(session, selection, None)

        if len(headers) == 0:
            req.write("<h2>No results match selection</h2>")
            req.write("</body></html>")
            return apache.HTTP_OK

        req.write("<TABLE>")
        req.write('<TR class="tr_head">')
        req.write("<TH>UsageLog ID</TH>")
        req.write("<TH>Filename</TH>")
        req.write("<TH>Data Label</TH>")
        req.write("<TH>User</TH>")
        req.write("<TH>Permission</TH>")
        req.write("<TH>Feature Used</TH>")
        req.write("<TH>IP addr</TH>")
        req.write("<TH>UT DateTime</TH>")
        req.write("<TH>HTTP Status</TH>")
        req.write("</TR>")

        even = False
        for header in headers:
            query = session.query(FileDownloadLog)\
                            .filter(FileDownloadLog.diskfile_filename == header.diskfile.filename)\
                            .filter(FileDownloadLog.diskfile_file_md5 == header.diskfile.file_md5)
            for fdl in query:
                even = not even
                req.write('<TR class="%s">' % ('tr_even' if even else 'tr_odd'))
                req.write('<TD><a href="/usagedetails/%d">%d</a></TD>' % (fdl.usagelog_id, fdl.usagelog_id))
                req.write('<TD>%s</TD>' % header.diskfile.filename)
                req.write('<TD>%s</TD>' % header.data_label)
                if fdl.usagelog.user_id:
                    user = session.query(User).filter(User.id == fdl.usagelog.user_id).one()
                    html = "%d: %s" % (user.id, user.username)
                    if user.gemini_staff:
                        html += " (Staff)"
                    req.write('<TD>%s</TD>' % html)
                else:
                    req.write('<TD>Anonymous</TD>')
                permission = ""
                if fdl.pi_access: permission += 'PI '
                if fdl.released: permission += 'Released '
                if fdl.staff_access: permission += 'Staff '
                if fdl.magic_access: permission += 'Magic '
                if fdl.eng_access: permission += 'Eng '
                if not fdl.canhaveit: permission += 'DENIED '
                req.write('<TD>%s</TD>' % permission)
                req.write('<TD>%s</TD>' % fdl.usagelog.this)
                req.write('<TD>%s</TD>' % fdl.usagelog.ip_address)
                req.write('<TD>%s</TD>' % fdl.ut_datetime)
                req.write('<TD>%s</TD>' % fdl.usagelog.status_string())
                req.write('</TR>')

        req.write('</TABLE>')

    finally:
        session.close()

    return apache.HTTP_OK

usagestats_header = """
<tr class='tr_head'>
<th></th>
<th colspan=2>Site Hits</th>
<th colspan=2>Searches</th>
<th colspan=2>PI Downloads</th>
<th colspan=2>Public Downloads</th>
<th colspan=2>Anonymous Downloads</th>
<th colspan=2>Staff Downloads</th>
<th colspan=2>Total Downloads</th>
<th>Failed Downloads</th>
<th colspan=2>Uploads</th>
</tr>
<tr class='tr_head'>
<th>Period</th>
<th>ok</th><th>fail</th>
<th>ok</th><th>fail</th>
<th>files</th><th>gb</th>
<th>files</th><th>gb</th>
<th>files</th><th>gb</th>
<th>files</th><th>gb</th>
<th>files</th><th>gb</th>
<th>number</th>
<th>files</th><th>gb</th>
</tr>
"""

def usagestats(req):
    """
    Usage statistics:
    Site hits
    Searches
    Downloads:
      Proprietry data
      Public data logged in
      Public data not logged in
    Ingests

    Generate counts per year, per week and per day
    """

    session = sessionfactory()

    try:
        # Need to be logged in as gemini staff to do this
        user = userfromcookie(session, req)
        if user is None or user.gemini_staff is False:
            return apache.HTTP_FORBIDDEN

        req.content_type = "text/html"
        req.write('<!DOCTYPE html><html><head>')
        req.write('<meta charset="UTF-8">')
        req.write('<link rel="stylesheet" href="/htmldocs/table.css">')
        req.write("<title>Usage Statistics</title>")
        req.write("</head>\n")
        req.write("<body>")
        req.write("<h1>Usage Statistics</h1>")

        first, last = session.query(func.min(UsageLog.utdatetime),
                                    func.max(UsageLog.utdatetime)).first()

        groups = (('Per Year', build_query(session, 'year')),
                  ('Per Week', build_query(session, 'week', first)),
                  ('Per Day',  build_query(session, 'day', first)))

        for header, query in groups:
            req.write("<h2>%s</h2>" % header)
            req.write('<TABLE>')
            req.write(usagestats_header)
            even = True
            header = True
            for result in query:
                even = not even
                req.write(render_usagestats_row(result, tr_class=('tr_even' if even else 'tr_odd')))
            req.write('</TABLE>')

        req.write('<H2>Within the last 90 days...</H2>')
        end = datetime.datetime.utcnow()
        interval = datetime.timedelta(days=90)
        start = end - interval

        req.write('<h3>Most inquisitive Users</h3>')
        query = session.query(UsageLog.user_id, func.count(1)).filter(UsageLog.this=='searchform')
        query = query.filter(UsageLog.utdatetime >= start).filter(UsageLog.utdatetime < end)
        query = query.group_by(UsageLog.user_id).order_by(desc(func.count(1))).limit(10)
        results = query.all()
        req.write('<TABLE>')
        req.write('<TR class="tr_head">')
        req.write('<TH>User</TH>')
        req.write('<TH>Searches</TH>')
        req.write('</TR>')
        even = False
        for result in results:
            even = not even
            tr_class = 'tr_even' if even else 'tr_odd'
            req.write('<TR class="%s">' % tr_class)
            if result[0]:
                user = session.query(User).filter(User.id == result[0]).one()
                name = user.username
                if user.gemini_staff:
                    name += " (Staff)"
            else:
                name = "Anonymous"
            req.write('<TD>%s</TD>' % name)
            req.write('<TD>%s</TD>' % result[1])
            req.write('</TR>')
        req.write('</TABLE>')

        req.write('<h3>Most hungry Users</h3>')
        query = session.query(UsageLog.user_id, func.sum(UsageLog.bytes)).filter(UsageLog.this=='download')
        query = query.filter(UsageLog.utdatetime >= start).filter(UsageLog.utdatetime < end)
        query = query.group_by(UsageLog.user_id).order_by(desc(func.sum(UsageLog.bytes))).limit(10)
        results = query.all()
        req.write('<TABLE>')
        req.write('<TR class="tr_head">')
        req.write('<TH>User</TH>')
        req.write('<TH>GB</TH>')
        req.write('</TR>')
        even = False
        for result in results:
            even = not even
            tr_class = 'tr_even' if even else 'tr_odd'
            req.write('<TR class="%s">' % tr_class)
            if result[0]:
                user = session.query(User).filter(User.id == result[0]).one()
                name = user.username
                if user.gemini_staff:
                    name += " (Staff)"
            else:
                name = "Anonymous"
            req.write('<TD>%s</TD>' % name)
            gb = result[1] / 1.0E9
            req.write('<TD>%.2f</TD>' % gb)
            req.write('</TR>')
        req.write('</TABLE>')

    finally:
        session.close()

    return apache.HTTP_OK

def render_usagestats_row(result, tr_class=''):
    """
    Generates an html table row giving the stats
    if header=true, generates the header row
    if tr_class, sets that class on the <TR>
    """

    def bytes_to_GB(bytes):
        return int(bytes) / 1.0E9

    if tr_class:
        html = '<TR class=%s>' % tr_class
    else:
        html = '<TR>'

    html += '<TD>%s</TD>' % (result.date)

    html += '<TD>%d</TD><TD>%d</TD>' % (result.hit_ok, result.hit_fail)
    html += '<TD>%d</TD><TD>%d</TD>' % (result.search_ok, result.search_fail)
    html += '<TD>%s</TD><TD>%.2f</TD>' % (result.pi_down, bytes_to_GB(result.pi_bytes))
    html += '<TD>%s</TD><TD>%.2f</TD>' % (result.public_down, bytes_to_GB(result.public_bytes))
    html += '<TD>%s</TD><TD>%.2f</TD>' % (result.anon_down, bytes_to_GB(result.anon_bytes))
    html += '<TD>%s</TD><TD>%.2f</TD>' % (result.staff_down, bytes_to_GB(result.staff_bytes))
    html += '<TD>%s</TD><TD>%.2f</TD>' % (result.total_down, bytes_to_GB(result.total_bytes))
    html += '<TD>%s</TD>' % result.failed_down
    html += '<TD>%d</TD><TD>%.2f</TD>' % (result.up, bytes_to_GB(result.up_bytes))

    html += '</TR>'

    return html

############################################################################################################

##     ##                            #                 #######
##     ##                           #                  ##     ##
##     ##  #####  # ###   #####     # ####   #####     ##     ## # ###   ####   ####   #####  # ###   #####
######### #     # ##   # #     #    ##    # #     #    ##     ## ##   #      # #    # #     # ##   # #
##     ## ######  #      ######     #     # ######     ##     ## #      #### # #    # #     # #    #  #####
##     ## #       #      #          #     # #          ##     ## #     #    ## #   ## #     # #    #       #
##     ##  #####  #       #####      #####   #####     #######   #      ######  ### #  #####  #    #  #####
                                                                                    #
                                                                                ####

############################################################################################################

# What follows is a function that bulds a rather complicated SQL query, written to optimize the call to
# usagestats, and as a testbed for other optimizations. This optimization is not really that needed, because
# usagestats is not called often, but the original function triggered multiple database queries per rendered
# row, and I (Ricardo) wanted to remove that behaviour.
#
# Understanding it SEEMS not to easy, but that's just because of the size. We'll be providing plenty of
# documentation to let future maintainers know # what to touch, and where.

UsageResult = namedtuple('UsageResult',
                (
                 'date',         # String representation of the summarized period
                 'hit_ok',       # Number of successful queries (HTTP Status 200)
                 'hit_fail',     # Number of non-successful queries
                 'search_ok',    # Number of successful queries involving /searchform
                 'search_fail',  # Number of non-successful queries involving /searchform
                 'total_down',   # Total downloaded files
                 'total_bytes',  # Total downloaded bytes
                 'up',           # Total uploaded files
                 'up_bytes',     # Total uploaded bytes
                 'pi_down',      # Total files downloaded by a PI user
                 'pi_bytes',     # Total bytes downloaded by a PI user
                 'staff_down',   # Total files downloaded by Gemini staff
                 'staff_bytes',  # Total bytes downloaded by Gemini staff
                 'public_down',  # Total released files downloaded by non-anonymous users
                 'public_bytes', # Total released-image bytes downloaded by non-anonymous users
                 'anon_down',    # Total files downloaded by an anonymous user
                 'anon_bytes',   # Total bytes downloaded by an anonymous user
                 'failed_down'   # Total failed downloads
                 ))

def build_query(session, period, since=None):
    '''This generator creates a query to tally usage stats, grouped by `period` (which can be
       'year', 'week', or 'day'. The objective is to return a collection of UsageResult, one
       per period, sumarizing the corresponding statistics. The definition of the UsageResult
       namedtuple explains each field.

       Both 'week' and 'day' must speficy `since`, to limit the amount of returned data. 'year'
       will work over all the data set.'''

    # This little function we'll use later to cast many of the results into integers. This is mainly
    # to translate booleans (True, False) into numbers, because often a True means '1 of this'. Thus,
    # we can use the result later in sums and products.
    def to_int(expr, big=False):
        return cast(expr, Integer if not big else BigInteger)

    # Note that we're using 'IS TRUE' here. This is a common theme across the whole query. The reason
    # for using 'IS' (identity) instead of '=' (equality) is that NULL values ARE NOT taken into account
    # for equality tests: we'd get a NULL out of it, and we want a boolean.
    #
    # Our database contains NULL in plenty of places where you'd expect to find False, so it makes sense
    # to use this test and be sure.
    RELEASED_FILE=to_int(FileDownloadLog.released.is_(True))

    # Subquery that summarizes downloads and bytes. This is needed because the relation between usagelog
    # and filedownloadlog is of one-to-many: filedownloadlog details the files (one per entry) for a
    # download query which shows only once in usagelog. If we wouldn't perform this subquery, when joining
    # usagelog on the left (we'll do it later), the final query would show more rows than expected,
    # resulting in bogus statistics.
    #
    # The subquery is rather simple, otherwise. The following code is roughly equivalent to:
    #
    #   (
    #    SELECT   ul.id AS ulid, pi_access, staff_access, COUNT(1) AS `count`, SUM(diskfile_file_size) AS bytes,
    #             SUM(released_file) AS released,
    #             SUM(released_file * diskfile_file_size) AS released_bytes
    #    FROM     filedownloadlog AS fdl JOIN usagelog AS ul ON fdl.usagelog_id = ul.id
    #    GROUP BY ul.id, fdl.pi_access, fdl.staff_access
    #   ) AS donwload_stats
    #
    # Note that 'released_file' is not a field in filedownloadlog. It's the operation represented by
    # RELEASED_FILE (see above), which is translated as:
    #
    #   CAST(filedownloadlog.released IS true AS integer)
    #
    # Which, as explained before, gets us a number, useful in sums and products. At the end of the day, this
    # query is giving us the following info:
    #
    #  - there was a petition for download                       (ul.id)
    #  - was it performed by a PI or Gemini staff?               (pi_access, staff_access - these are booleans)
    #  - how many files were downloaded in total?                (count)
    #  - how many bytes in total?                                (bytes)
    #  - how many of those files are out of proprietary period?  (released)
    #  - and how many bytes do those represent, you said?        (released_bytes)
    download_query = session.query(UsageLog.id.label('ulid'),
                                   FileDownloadLog.pi_access.label('pi_access'),
                                   FileDownloadLog.staff_access.label('staff_access'),
                                   func.count(FileDownloadLog.id).label('count'),
                                   func.sum(FileDownloadLog.diskfile_file_size).label('bytes'),
                                   func.sum(RELEASED_FILE).label('released'),
                                   func.sum(RELEASED_FILE * FileDownloadLog.diskfile_file_size).label('released_bytes'))\
                            .select_from(join(FileDownloadLog, UsageLog))\
                            .group_by(UsageLog.id, FileDownloadLog.pi_access,
                                                   FileDownloadLog.staff_access)\
                            .cte(name='download_stats')

    # Subquery that summarizes uploads and bytes. The rationale for this subquery would be the same as
    # for download_query, as the relationship between usagelog and fileuploadlog is technically a
    # one-to-many. In reality, though, the current implementation accepts only single files per upload,
    # so there's only one entry per upload. It doesn't hurt to generalize, though, and gives us an
    # appropriate target for the big fat JOIN that will be performed later. Plus, if we ever implement
    # uploading tarballs, we get that for free (aren't we smart?)
    #
    # This is basically equivalent to:
    #
    #   (
    #    SELECT   ul.id AS ulid, COUNT(1) as `count`, SUM(ful.bytes) AS bytes
    #    FROM     fileuploadlog AS ful JOIN usagelog AS ul ON ful.usagelog_id = ul.id
    #    GROUP BY ul.id
    #   ) AS upload_stats
    #
    # The name for the fields are equivalent to those of the download query; just substitute
    # 'downloaded' for 'uploaded'.
    upload_query = session.query(UsageLog.id.label('ulid'),
                                 func.count(FileUploadLog.id).label('count'),
                                 func.sum(FileUploadLog.size).label('bytes'))\
                          .select_from(join(FileUploadLog, UsageLog))\
                          .group_by(UsageLog.id)\
                          .cte(name='upload_stats')

    # Now, THIS unassuming query fragment is the core join that relates all the usage statistics.
    # It's equivalent to:
    #
    #    ...
    #    FROM (usagelog AS ul LEFT JOIN download_stats AS ds ON ul.id = ds.ulid)
    #                         LEFT JOIN upload_stats AS us ON ul.id = us.ulid
    #    ...
    #
    # Notice that we're doing Left Outer Joins here. This is VERY IMPORTANT. Doing a regular
    # Inner (natural) Join would return rows ONLY where a usagelog entry has a corresponding
    # download entry (or entries)... AND a corresponding upload entry.
    #
    # Which is impossible
    #
    # It would be bad enough even if we had only downloads, because we'de be limited to only
    # download queries, and we want all of them. In any case, what we get out of this join
    # operation is one row per usagelog entry, with (potentally) extra data if there was a
    # download or an upload. Otherwise, all those extra columns will be NULL, which is OK.
    the_join = join(join(UsageLog, download_query, UsageLog.id == download_query.c.ulid,
                         isouter=True),
                    upload_query, UsageLog.id == upload_query.c.ulid,
                    isouter=True)

    # Now comes the (potentially) most confusing part. We want to group the entries of the
    # join we just defined. And the grouping will be done according to one out of three
    # criteria:
    #
    #  - per year
    #  - per week (with the first day of the first week starting in the day passed in 'since',
    #              may not necessarily be Sunday - or Monday, for those of you in countries with
    #              a non-Sunday first day of the week)
    #  - per day
    #
    # To do this, in the following piece of code we create a master query that incorporates
    # the_join. Notice, though, that we're only querying for one column (the one that defines
    # the period for the row). That's OK. All the other columns are common to the different
    # queries, and we'll add them later using the `add_columns` method of the SQLAlchemy
    # query object.
    if period == 'year':
        # Simple enough. We extract the year component out of the usagelog.utdatetime, and
        # use that information to group the rows. Nothing complicated here.
        #
        # The equivalent query here would be then:
        #
        #    SELECT   EXTRACT(YEAR FROM utdatetime)
        #    FROM     (usagelog AS ul LEFT JOIN download_stats AS ds ON ul.id = ds.ulid)
        #                            LEFT JOIN upload_stats AS us ON ul.id = us.ulid)
        #    ORDER BY 1  -- Using the column position to avoid repeating the whole
        #    GROUP BY 1  -- EXTRACT(....)
        #
        ULYEAR = extract('YEAR', UsageLog.utdatetime)
        query = session.query(to_int(ULYEAR)).select_from(the_join).order_by(ULYEAR).group_by(ULYEAR)
    elif period in ('week', 'day'):
        # These two are a bit more complicated. It's easy to group by year, because it's
        # the slowest changing member of the date component... and never repeats (within
        # a specified calendar convention, that is)
        #
        # Week and day numbers, though, repeat rather often, meaning that we CANNOT use
        # them straight. Instead, we'll define an auxiliary 'period' table, with entries
        # defining the beginning and end of one. That will let us use the 'BETWEEN' operator
        # as grouping criterion.

        since = cast(since, Date) # Just to make sure that we have a date, not a timestamp

        # oneinterval is the only variable thing here. It depends on the input arguments
        # and can be one of:
        #
        #   - INTERVAL '1 week'
        #   - INTERVAL '1 day'
        oneinterval = cast('1 {0}'.format(period), Interval)
        onemsecond = cast('1 microsecond', Interval)

        # The following describes an aliased table (in SQLAlchemy terminology; for
        # PostgreSQL we would be talking about a CTE - Common Table Expression).
        # Also known as 'WITH query' Very useful to break down complicated queries
        # into simple ones.
        #
        # This one will prepare a temporary table of periods for us:
        #
        #    SELECT generate_series(first_date, last_date, INTERVAL '...') as start
        #
        # `start` is the name of the column in this subquery. `timeperiod` (the name
        # of the "aliased" table) will be used in the final query.
        intervals = func.generate_series(since, func.now(), oneinterval).label('start')
        aliased_intervals = aliased(session.query(intervals).subquery(), 'timeperiod')

        # Here we define the starting and ending points of a period. `start` comes
        # from the `start` column from timeperiod.
        # `end` is equivalent to (start + INTERVAL '...' - INTERVAL '1 microsecond').
        # We substract one microsecond because the operator 'BETWEEN' works on
        # closed ranges, meaning that it will include both ends. Substracting that
        # microsecond will get us a period like:
        #
        #  2015-02-14 00:00:00 - 2015-02-20 23:59:59.999999
        #
        # which should be more than enough precision for our needs. This won't work
        # if a query was placed during a leap second, but tough luck...
        start = aliased_intervals.c.start
        end = (start + oneinterval) - onemsecond

        # One more LEFT join! This one is to incorporate the whole list of periods
        # to the query. Again, we use a LEFT join to allow the retrieval of periods
        # with no activity whatsoever (an inner join would skip them). Equivalent to:
        #
        #   ... FROM timeperiod AS tp LEFT JOIN the_main_join AS tmj ON ul.utdatetime BETWEEN start AND end ...
        #
        # Here `ul` comes from the core JOIN that we defined befure, and `start` and `end`
        # are the expressions we just defined.
        more_join = join(aliased_intervals, the_join,
                         between(UsageLog.utdatetime, start, end),
                         isouter=True)

        # Finally, the query. This translates to:
        #
        #    WITH     (SELECT generate_series(first_date, last_date, INTERVAL '...') as start)
        #             AS timeperiod
        #    SELECT   (start || ' - ' || end)
        #    FROM     timeperiod AS tp LEFT JOIN
        #             ((usagelog AS ul LEFT JOIN download_stats AS ds ON ul.id = ds.ulid)
        #                              LEFT JOIN upload_stats AS us ON ul.id = us.ulid))
        #             ON ul.utdatetime BETWEEN start AND end
        #    ORDER BY start
        #    GROUP BY start
        #
        if period == 'week':
            period_element = cast(cast(start, Date), String) + ' - ' + cast(cast(end, Date), String)
        else:
            period_element = cast(start, Date)
        query = session.query(period_element)\
                       .select_from(more_join)\
                       .order_by(start)\
                       .group_by(start)
    else:
        raise RuntimeException('No valid period specified')

    # The rest of the the function defines some auxiliary terms that we'll use to build
    # the summarizing columns, which is what WE REALLY WANT to extract. They're not
    # complex and add nothing to the logic of the query. They're simply added to the
    # retrieved columns. All the information to figure out what info are we working with
    # has been described before.
    STATUS_200 = (UsageLog.status == 200)
    THIS_SEARCH = (UsageLog.this == "searchform")
    HIT_OK = to_int(STATUS_200.is_(True))
    HIT_FAIL = to_int(STATUS_200.is_(False))
    SEARCH_OK = to_int(and_(STATUS_200, THIS_SEARCH).is_(True))
    SEARCH_FAIL = to_int(and_(STATUS_200.is_(True), THIS_SEARCH.is_(False)))

    DOWNLOAD_PERFORMED = and_(STATUS_200.is_(True), download_query.c.ulid.isnot(None)).is_(True)
    DOWNLOAD_FAILED    = and_(STATUS_200.is_(False), download_query.c.ulid.isnot(None)).is_(True)
    UPLOAD_PERFORMED   = and_(STATUS_200.is_(True), upload_query.c.ulid.isnot(None)).is_(True)

    FILE_COUNT = to_int(func.coalesce(download_query.c.count, 0))
    PUBFILE_COUNT = to_int(func.coalesce(download_query.c.released, 0))
    DOWNBYTE_COUNT = to_int(func.coalesce(download_query.c.bytes, 0), big=True)
    UPBYTE_COUNT = to_int(func.coalesce(upload_query.c.bytes, 0), big=True)

    COUNT_DOWNLOAD = to_int(DOWNLOAD_PERFORMED) * FILE_COUNT
    COUNT_FAILED = to_int(DOWNLOAD_FAILED)
    COUNT_UPLOAD = to_int(UPLOAD_PERFORMED)
    PI_DOWNLOAD = to_int(and_(DOWNLOAD_PERFORMED, download_query.c.pi_access.is_(True)))
    STAFF_DOWNLOAD = to_int(and_(DOWNLOAD_PERFORMED, download_query.c.staff_access.is_(True)))
    ANON_DOWNLOAD = to_int(and_(DOWNLOAD_PERFORMED, UsageLog.user_id.is_(None)))

    TOTAL_BYTES = COUNT_DOWNLOAD * to_int(func.coalesce(UsageLog.bytes, 0), big=True)
    PUBFILE_BYTES = to_int(func.coalesce(download_query.c.released_bytes, 0), big=True)

    q = query.add_columns(func.sum(HIT_OK).label('hits_ok'), func.sum(HIT_FAIL).label('hits_fail'),
                          func.sum(SEARCH_OK).label('search_ok'), func.sum(SEARCH_FAIL).label('search_fail'),
                          func.sum(COUNT_DOWNLOAD).label('downloads_total'), func.sum(TOTAL_BYTES).label('bytes_total'),
                          func.sum(COUNT_UPLOAD).label('uploads_total'), func.sum(COUNT_UPLOAD * UPBYTE_COUNT).label('ul_bytes_total'),
                          func.sum(PI_DOWNLOAD * FILE_COUNT).label('pi_downloads'), func.sum(PI_DOWNLOAD * DOWNBYTE_COUNT).label('pi_dl_bytes'),
                          func.sum(STAFF_DOWNLOAD * FILE_COUNT).label('staff_downloads'), func.sum(STAFF_DOWNLOAD * DOWNBYTE_COUNT).label('staff_dl_bytes'),
                          func.sum(PUBFILE_COUNT).label('public_downloads'), func.sum(PUBFILE_BYTES).label('public_dl_bytes'),
                          func.sum(ANON_DOWNLOAD * FILE_COUNT).label('anon_downloads'), func.sum(ANON_DOWNLOAD * DOWNBYTE_COUNT).label('anon_dl_bytes'),
                          func.sum(COUNT_FAILED).label('failed'))

    # Yield the results. Yay! This function is a generator ;-)
    for result in q:
        yield UsageResult(*result)
