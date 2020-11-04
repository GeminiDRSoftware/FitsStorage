
import pytest
import sqlalchemy

import fits_storage
from fits_storage.orm.notification import Notification
from fits_storage.web.list_headers import list_headers, list_obslogs
from fits_storage.web.logcomments import log_comments
from fits_storage.web.notification import notification
from tests.web_helper import MockContext


@pytest.mark.usefixtures("rollback")
def test_notification(session, monkeypatch):
    notif = Notification('oldlabel')
    session.add(notif)
    session.flush()

    form_data = {
        'newlabel-%d' % notif.id: 'newlabel',
        'newsel-%d' % notif.id: 'newsel',
        'newpiemail-%d' % notif.id: 'newpiemail',
        'newngoemail-%d' % notif.id: 'newngoemail',
        'newcsemail-%d' % notif.id: 'newcsemail',
        'internal-%d' % notif.id: 'Yes',
    }

    mock_context = MockContext(session, method='GET', form_data=form_data)

    def _mock_get_context(initialize=True):
        return mock_context

    monkeypatch.setattr(sqlalchemy.orm.session.Session, 'commit', sqlalchemy.orm.session.Session.flush)
    monkeypatch.setattr(fits_storage.web.user, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.templating, "get_context", _mock_get_context)
    monkeypatch.setattr(fits_storage.web.notification, "get_context", _mock_get_context)

    notification()

    assert(notif.label == 'newlabel')
    assert(notif.selection == 'newsel')
    assert(notif.piemail == 'newpiemail')
    assert(notif.ngoemail == 'newngoemail')
    assert(notif.csemail == 'newcsemail')
    assert(notif.internal == True)

    assert('FITS Storage new data email' in mock_context.resp.stuff)

    form_data['internal-%d' % notif.id] = 'No'
    mock_context = MockContext(session, method='GET', form_data=form_data)
    notification()
    assert(notif.internal == False)

    form_data = {'newone-0': 'newone'}
    mock_context = MockContext(session, method='GET', form_data=form_data)
    notification()
    newone = session.query(Notification).filter(Notification.label == 'newone').one()
    assert(newone is not None)

    session.rollback()
