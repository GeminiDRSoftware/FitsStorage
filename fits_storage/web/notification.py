"""
This module contains the notification html generator function, and odb import via web function
"""
from ..orm import sessionfactory
from ..orm.notification import Notification
from ..fits_storage_config import use_as_archive
from .user import userfromcookie
from ..utils.userprogram import got_magic
from ..utils.notifications import ingest_odb_xml

from mod_python import apache, util

def notification(req):
    """
    This is the email notifications page. It's both to show the current notifcation list and to update it.
    """
    req.content_type = "text/html"
    req.write("<html>")
    req.write("<head><title>FITS Storage new data email notification list</title></head>")
    req.write("<body>")
    req.write("<h1>FITS Storage new data email notification list</h1>")
    req.write("<P>There is a <a href='htmldocs/notificationhelp.html'>help page</a> if you're unsure how to use this.</P>")
    req.write("<HR>")

    session = sessionfactory()
    try:
        # On archive systems, need to be logged in as gemini staff to do this
        if use_as_archive:
            user = userfromcookie(session, req)
            if user is None or user.gemini_staff is False:
                return apache.HTTP_FORBIDDEN

        # Process form data first
        formdata = util.FieldStorage(req)
        # req.write(str(formdata))
        for key in formdata.keys():
            field = key.split('-')[0]
            nid = int(key.split('-')[1])
            value = formdata[key].value
            if nid:
                #debug print:
                #req.write("<H1>id=%d, field=%s. value=%s</H1>" % (id, field, value))
                notif = session.query(Notification).filter(Notification.id == nid).first()
                if field == 'delete' and value == 'Yes':
                    session.delete(notif)
                    session.commit()
                    break
                else:
                    if field == 'newlabel':
                        notif.label = value
                    if field == 'newsel':
                        notif.selection = value
                    if field == 'newto':
                        notif.to = value
                    if field == 'newcc':
                        notif.cc = value
                    if field == 'internal':
                        if value == 'Yes':
                            notif.internal = True
                        if value == 'No':
                            notif.internal = False

            if field == 'newone':
                # Add a new notification to the database
                notif = Notification(value)
                session.add(notif)

            session.commit()

        # Get a list of the notifications in the table
        query = session.query(Notification).order_by(Notification.id)
        notifs = query.all()

        for notif in notifs:
            req.write("<H2>Notification ID: %d - %s</H2>" % (notif.id, notif.label))
            req.write("<UL>")
            req.write("<LI>Data Selection: %s</LI>" % notif.selection)
            req.write("<LI>Email To: %s</LI>" % notif.to)
            req.write("<LI>Email CC: %s</LI>" % notif.cc)
            req.write("<LI>Gemini Internal: %s</LI>" % notif.internal)
            req.write("</UL>")

            # The form for modifications
            req.write('<FORM action="/notification" method="post">')
            req.write('<TABLE>')

            mod_list = [['newlabel', 'Update notification label'],
                        ['newsel', 'Update data selection'],
                        ['newto', 'Update Email To'],
                        ['newcc', 'Update Email Cc'],
                        ['internal', 'Internal Email'],
                        ['delete', 'Delete']]
            for key in range(len(mod_list)):
                user = mod_list[key][0]+"-%d" % notif.id
                req.write('<TR>')
                req.write('<TD><LABEL for="%s">%s:</LABEL></TD>' % (user, mod_list[key][1]))
                if mod_list[key][0] == 'internal':
                    yeschecked = ""
                    nochecked = ""
                    if notif.internal:
                        yeschecked = "checked"
                    else:
                        nochecked = " checked"
                    req.write('<TD><INPUT type="radio" name="%s" value="Yes" %s>Yes</INPUT> ' % (user, yeschecked))
                    req.write('<INPUT type="radio" name="%s" value="No" %s>No</INPUT></TD>' % (user, nochecked))
                elif mod_list[key][0] == 'delete':
                    yeschecked = ""
                    nochecked = "checked"
                    req.write('<TD><INPUT type="radio" name="%s" value="Yes" %s>Yes</INPUT> ' % (user, yeschecked))
                    req.write('<INPUT type="radio" name="%s" value="No" %s>No</INPUT></TD>' % (user, nochecked))
                else:
                    req.write('<TD><INPUT type="text" size=32 name="%s"></INPUT></TD>' % user)
                req.write('</TR>')

            req.write('</TABLE>')
            req.write('<INPUT type="submit" value="Save"></INPUT> <INPUT type="reset"></INPUT>')
            req.write('</FORM>')
            req.write('<HR>')

        req.write('<HR>')
        req.write('<H2>Add a New Notification</H2>')
        req.write('<FORM action="/notification" method="post">')
        req.write('<LABEL for=newone-0>Label</LABEL> <INPUT type="text" size=32 name=newone-0></INPUT> <INPUT type="submit" value="Save"></INPUT> <INPUT type="reset"></INPUT>')
        req.write('</FORM>')

        req.write("</body></html>")
        return apache.HTTP_OK
    except IOError:
        pass
    finally:
        session.close()

def import_odb_notifications(req):
    """
    This takes xml from the ODB posted to it and imports it as notifications
    """

    # Only accept http posts
    if req.method != 'POST':
        return apache.HTTP_NOT_ACCEPTABLE

    # Must have secret cookie
    if not got_magic(req):
        return apache.HTTP_NOT_ACCEPTABLE

    # OK, get the payload from the POST data
    xml = req.read()

    session = sessionfactory()
    try:
        # Process it
        report = ingest_odb_xml(session, xml)

        # Write back the report
        req.content_type = "text/plain"
        for l in report:
            req.write(l)
            req.write('\n')

    except IOError:
        pass
    finally:
        session.close()


    return apache.HTTP_OK

