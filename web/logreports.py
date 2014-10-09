"""
This module handles the web 'logreports' functions - presenting data from the usage, query, upload and download logs
"""
import datetime
import dateutil.parser

from sqlalchemy import desc, func, join

from orm import sessionfactory
from orm.usagelog import UsageLog
from orm.querylog import QueryLog
from orm.downloadlog import DownloadLog
from orm.filedownloadlog import FileDownloadLog
from orm.fileuploadlog import FileUploadLog
from orm.user import User

from web.user import userfromcookie
from web.selection import getselection, queryselection, sayselection
from web.list_headers import list_headers

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
        oneday = datetime.timedelta(days=1)
        yesterday = today - oneday
        start = today.isoformat()
        end = yesterday.isoformat()
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
        req.write("<TR><TD>Notes:</TD><TD>%s</TD></TR>" % usagelog.notes)
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
                    req.write("<TD>%.2f</TD>" % fileuploadlog.size / (1000*tdel.total_seconds()))
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
                    req.write("<TD>%.2f</TD>" % fileuploadlog.size / (1000*tdel.total_seconds()))
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

    return apache.OK

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
            return apache.OK

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
            query = session.query(FileDownloadLog).filter(FileDownloadLog.diskfile_filename == header.diskfile.filename).filter(FileDownloadLog.diskfile_file_md5 == header.diskfile.file_md5)
            fdls = query.all()
            for fdl in fdls:
                even = not even
                if even:
                    req.write('<TR class="tr_even">')
                else:
                    req.write('<TR class="tr_odd">')
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

    return apache.OK

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

        first = session.query(func.min(UsageLog.utdatetime)).first()[0]
        last = session.query(func.max(UsageLog.utdatetime)).first()[0]
   
        req.write("<h2>Per Year</h2>")
        year = first.year
        start = datetime.datetime(year, 1, 1, 0, 0, 0)
        end = datetime.datetime(year+1, 1, 1, 0, 0, 0)
        req.write('<TABLE>')
        req.write(render_usagestats_row(session, start, end, header=True, tr_class='tr_head'))
        req.write(render_usagestats_row(session, start, end, header=False, tr_class='tr_odd'))
        even = False
        while end < last:
            even = not even
            tr_class = 'tr_even' if even else 'tr_odd'
            year += 1
            start = datetime.datetime(year, 1, 1, 0, 0, 0)
            end = datetime.datetime(year+1, 1, 1, 0, 0, 0)
            req.write(render_usagestats_row(session, start, end, header=False, tr_class=tr_class))
        req.write('</TABLE>')

        req.write("<h2>Per Week</h2>")
        delta = datetime.timedelta(days=7)
        start = datetime.datetime(first.year, first.month, first.day, 0, 0, 0)
        end = start + delta
        req.write('<TABLE>')
        req.write(render_usagestats_row(session, start, end, header=True, tr_class='tr_head'))
        req.write(render_usagestats_row(session, start, end, header=False, tr_class='tr_odd'))
        even = False
        while end < last:
            even = not even
            tr_class = 'tr_even' if even else 'tr_odd'
            start = start + delta
            end = end + delta
            req.write(render_usagestats_row(session, start, end, header=False, tr_class=tr_class))
        req.write('</TABLE>')

        req.write("<h2>Per Day</h2>")
        delta = datetime.timedelta(days=1)
        start = datetime.datetime(first.year, first.month, first.day, 0, 0, 0)
        end = start + delta
        req.write('<TABLE>')
        req.write(render_usagestats_row(session, start, end, header=True, tr_class='tr_head'))
        req.write(render_usagestats_row(session, start, end, header=False, tr_class='tr_odd'))
        even = False
        while end < last:
            even = not even
            tr_class = 'tr_even' if even else 'tr_odd'
            start = start + delta
            end = end + delta
            req.write(render_usagestats_row(session, start, end, header=False, tr_class=tr_class))
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

    return apache.OK

def render_usagestats_row(session, start, end, header=False, tr_class=''):
    """
    Generates an html table row giving the stats
    if header=true, generates the header row
    if tr_class, sets that class on the <TR>
    """

    if tr_class:
        html = '<TR class=%s>' % tr_class
    else:
        html = '<TR>'

    if header:
        html += '<TH colspan=2>Period</TH>'
        html += '<TH colspan=2>Site Hits</TH>'
        html += '<TH colspan=2>Searches</TH>'
        html += '<TH colspan=2>PI Downloads</TH>'
        html += '<TH colspan=2>Public Downloads</TH>'
        html += '<TH colspan=2>Anonymous Downloads</TH>'
        html += '<TH colspan=2>Staff Downloads</TH>'
        html += '<TH colspan=2>Total Downloads</TH>'
        html += '<TH>Failed Downloads</TH>'
        html += '<TH colspan=2>Uploads</TH>'
        html += '</TR>'
        if tr_class:
            html += '<TR class=%s>' % tr_class
        else:
            html += '<TR>'
        html += '<TH>From</TH><TH>To</TH>'
        html += '<TH>OK</TH><TH>Fail</TH>'
        html += '<TH>OK</TH><TH>Fail</TH>'
        html += '<TH>Files</TH><TH>GB</TH>'
        html += '<TH>Files</TH><TH>GB</TH>'
        html += '<TH>Files</TH><TH>GB</TH>'
        html += '<TH>Files</TH><TH>GB</TH>'
        html += '<TH>Files</TH><TH>GB</TH>'
        html += '<TH>Number</TH>'
        html += '<TH>Files</TH><TH>GB</TH>'
        html += '</TR>'
        return html

    usage = calculate_usagestats(session, start, end)

    html += '<TD>%s</TD><TD>%s</TD>' % (start, end)

    html += '<TD>%d</TD><TD>%d</TD>' % (usage['site_hits']['OK'], usage['site_hits']['fail'])
    html += '<TD>%d</TD><TD>%d</TD>' % (usage['searches']['OK'], usage['searches']['fail'])

    html += '<TD>%d</TD><TD>%.2f</TD>' % (usage['pi_downloads']['files'], usage['pi_downloads']['GB'])
    html += '<TD>%d</TD><TD>%.2f</TD>' % (usage['public_downloads']['files'], usage['public_downloads']['GB'])
    html += '<TD>%d</TD><TD>%.2f</TD>' % (usage['anon_downloads']['files'], usage['anon_downloads']['GB'])
    html += '<TD>%d</TD><TD>%.2f</TD>' % (usage['staff_downloads']['files'], usage['staff_downloads']['GB'])
    html += '<TD>%d</TD><TD>%.2f</TD>' % (usage['total_downloads']['files'], usage['total_downloads']['GB'])

    html += '<TD>%d</TD>' % usage['failed_downloads']

    html += '<TD>%d</TD><TD>%.2f</TD>' % (usage['uploads']['files'], usage['uploads']['GB'])

    html += '</TR>'

    return html

def calculate_usagestats(session, start, end):
    """
    start and end are datetime objects in UTC.
    Returns a dict containing the stats for that period
    'site_hits': {'OK': int, 'fail': int}
    'searches': {'OK': int, 'fail': int}
    'pi_downloads': {'nfiles': int, 'GB': float}
    'public_downloads': {'files': int, 'GB': float}
    'anon_downloads': {'files': int, 'GB': float}
    'staff_downloads': {'files': int, 'GB': float}
    'total_downloads': {'files': int, 'GB': float}
    'failed_downloads': int
    'uploads': {'files': int, 'GB': float}
    """

    retary = {}
    usagequery = session.query(UsageLog).filter(UsageLog.utdatetime >= start).filter(UsageLog.utdatetime < end)

    hitsok = usagequery.filter(UsageLog.status == 200).count()
    hitsfail = usagequery.filter(UsageLog.status != 200).count()
    retary['site_hits'] = {'OK': hitsok, 'fail': hitsfail}

    searchok = usagequery.filter(UsageLog.this=="searchform").filter(UsageLog.status == 200).count()
    searchfail = usagequery.filter(UsageLog.this=="searchform").filter(UsageLog.status != 200).count()
    retary['searches'] = {'OK': searchok, 'fail': searchfail}

    fdlquery = session.query(FileDownloadLog).select_from(join(FileDownloadLog, UsageLog)).filter(UsageLog.utdatetime >= start).filter(UsageLog.utdatetime < end)
    retary['failed_downloads'] = fdlquery.filter(UsageLog.status != 200).count()

    fdlquery = session.query(func.count(1), func.sum(UsageLog.bytes)).select_from(join(FileDownloadLog, UsageLog))
    fdlquery = fdlquery.filter(UsageLog.utdatetime >= start).filter(UsageLog.utdatetime < end)
    fdlquery = fdlquery.filter(UsageLog.status == 200)
    
    total = fdlquery.first()
    if total:
        retary['total_downloads'] = {'files': total[0], 'GB' : total[1]/1.0E9 if total[1] else 0}
    else:
        retary['total_downloads'] = {'files': 0, 'GB' : 0}

    staff = fdlquery.filter(FileDownloadLog.staff_access == True).first()
    if staff:
        retary['staff_downloads'] = {'files': staff[0], 'GB' : staff[1]/1.0E9 if staff[1] else 0}
    else:
        retary['staff_downloads'] = {'files': 0, 'GB' : 0}

    pi = fdlquery.filter(FileDownloadLog.pi_access == True).first()
    if pi:
        retary['pi_downloads'] = {'files': pi[0], 'GB' : pi[1]/1.0E9 if pi[1] else 0}
    else:
        retary['pi_downloads'] = {'files': 0, 'GB' : 0}

    pub = fdlquery.filter(FileDownloadLog.released == True).filter(UsageLog.user_id is not None).first()
    if pub:
        retary['public_downloads'] = {'files': pub[0], 'GB' : pub[1]/1.0E9 if pub[1] else 0}
    else:
        retary['public_downloads'] = {'files': 0, 'GB' : 0}

    anon = fdlquery.filter(UsageLog.user_id is None).first()
    if anon:
        retary['anon_downloads'] = {'files': anon[0], 'GB' : anon[1]/1.0E9 if anon[1] else 0}
    else:
        retary['anon_downloads'] = {'files': 0, 'GB' : 0}
    
    fulquery = session.query(FileUploadLog).select_from(join(FileUploadLog, UsageLog)).filter(UsageLog.utdatetime >= start).filter(UsageLog.utdatetime < end)
    uploads = fulquery.first()
    if uploads:
        retary['uploads'] = {'files': uploads[0], 'GB': uploads[1]/1.0E9 if uploads[1] else 0}
    else:
        retary['uploads'] = {'files': 0, 'GB': 0}

    return retary
