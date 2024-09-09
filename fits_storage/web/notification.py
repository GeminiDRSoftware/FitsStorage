"""
This module contains the notification html generator function,
and odb import via web function
"""
from fits_storage.server.orm.notification import Notification

from fits_storage.server.wsgi.context import get_context
from fits_storage.server.wsgi.returnobj import Return

from .user import needs_login, needs_cookie

from . import templating

from xml.parsers.expat import ExpatError

from fits_storage.config import get_config
fsc = get_config()


@needs_login(staff=True)
@templating.templated("notification.html")
def notification():
    """
    This is the email notifications page. It's both to show the current
    notification list and to update it.
    """

    ctx = get_context()
    session = ctx.session

    # Process form data first
    formdata = ctx.get_form_data()
    for key, value in list(formdata.items()):
        field = key.split('-')[0]
        nid = int(key.split('-')[1])
        value = value.value

        if nid:
            notif = session.query(Notification).\
                filter(Notification.id == nid).first()
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
