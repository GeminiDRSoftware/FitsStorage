# Very simple sanity checks for the server ORMs

from fits_storage.server.orm.usagelog import UsageLog
from fits_storage.server.orm.downloadlog import DownloadLog
from fits_storage.server.orm.filedownloadlog import FileDownloadLog
from fits_storage.server.orm.fileuploadlog import FileUploadLog
from fits_storage.server.orm.querylog import QueryLog


class Thing:
    # A trivial python container we can set attributes on to use as mock-up
    # instances of objects that we interrogate for values in the tests
    pass


def make_ctx():
    # Build a mockup ctx.req.env structure
    e = Thing()
    e.remote_ip = '1.2.3.4'
    e.user_agent = 'Smith'
    e.referrer = 'Switch'
    e.method = 'Phone'
    e.unparsed_uri = 'Geller'
    r = Thing()
    r.env = e
    ctx = Thing()
    ctx.req = r

    return ctx


def notes_helper(l):
    # Several of the log ORM classes have the same notes functionality
    assert l.notes is None

    l.add_note('Hello')
    assert l.notes == 'Hello'

    l.add_note('again')
    assert l.notes == 'Hello\nagain'



def test_orm_usagelog():
    ctx = make_ctx()

    ul = UsageLog(ctx)
    assert ul.ip_address == '1.2.3.4'
    assert ul.user_agent == 'Smith'
    assert ul.referer == 'Switch'
    assert ul.method == 'Phone'
    assert ul.uri == 'Geller'


def test_orm_downloadlog():
    # Make a mock up usage log id
    ul = Thing()
    ul.id = 123
    dl = DownloadLog(ul)

    assert dl is not None
    assert dl.usagelog_id == 123

    notes_helper(dl)


def test_orm_filedownloadlog():
    # Make a mock up usage log id
    ul = Thing()
    ul.id = 456
    fdl = FileDownloadLog(ul)

    assert fdl is not None
    assert fdl.usagelog_id == 456
    assert fdl.ut_datetime is not None

    notes_helper(fdl)


def test_orm_fileuploadlog():
    # Make a mock up usage log id
    ul = Thing()
    ul.id = 789
    ful = FileUploadLog(ul)

    assert ful.usagelog_id == 789

    notes_helper(ful)


def test_orm_querylog():
    # Make a mock up usage log id
    ul = Thing()
    ul.id = 234
    ql = QueryLog(ul)

    assert ql.usagelog_id == 234

    notes_helper(ql)

