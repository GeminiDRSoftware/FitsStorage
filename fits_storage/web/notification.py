"""
This module contains the notification html generator function, and odb import via web function
"""
from ..orm import session_scope
from ..orm.notification import Notification
from ..fits_storage_config import use_as_archive, magic_download_cookie

from ..utils.userprogram import got_magic
from ..utils.notifications import ingest_odb_xml
from ..utils.web import Context

from .user import needs_login

from . import templating

from mod_python import apache, util

@needs_login(staffer=True)
@templating.templated("notification.html", with_session = True)
def notification(session, req):
    """
    This is the email notifications page. It's both to show the current notifcation list and to update it.
    """

    # Process form data first
    formdata = util.FieldStorage(req)
    for key, value in formdata.items():
        field = key.split('-')[0]
        nid = int(key.split('-')[1])

        if nid:
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
                if field == 'newpiemail':
                    notif.piemail = value
                if field == 'newngoemail':
                    notif.ngoemail = value
                if field == 'newcsemail':
                    notif.csemail = value
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

    return dict(
        notifications = session.query(Notification).order_by(Notification.id),
        mod_list      = [('newlabel', 'Update notification label'),
                         ('newsel', 'Update data selection'),
                         ('newpiemail', 'Update PI Email'),
                         ('newngoemail', 'Update NGO Email'),
                         ('newcsemail', 'Update CS Email'),
                         ('internal', 'Internal Email'),
                         ('delete', 'Delete')]
        )

@needs_login(magic_cookies=[('gemini_fits_authorization', magic_download_cookie)], only_magic=True)
def import_odb_notifications(req):
    """
    This takes xml from the ODB posted to it and imports it as notifications
    """

    # Only accept http posts
    if req.method != 'POST':
        return apache.HTTP_NOT_ACCEPTABLE

    ctx = Context()

    # OK, get the payload from the POST data
    xml = ctx.req.raw_data

    with session_scope() as session:
        # Process it
        report = ingest_odb_xml(session, xml)

        # Write back the report
        ctx.req.content_type = "text/plain"
        ctx.resp.append_iterable(l + '\n' for l in report)

    return apache.HTTP_OK
