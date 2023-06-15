import pytest

from fits_storage.web.user import needs_login, needs_cookie

badcookie_message = "This resource can only be accessed by providing a valid " \
                    "magic cookie, which this request did not."

notloggedin_message = "You need to be logged in to access this resource"

notstaff_message = "You need to be logged in as Gemini Staff member to access" \
                   " this resource"

class FakeUser(object):
    username = None
    staff = None
    misc_upload = None
    superuser = None

    def __init__(self, username=None, staff=False, misc_upload=False,
                 superuser=False):
        self.username = username
        self.gemini_staff = staff
        self.misc_upload = misc_upload
        self.superuser = superuser

class FakeRespError(Exception):
    pass

class FakeResp(object):

    def client_error(self, code=None, content_type=None, annotate=None,
                     message=None):
        raise FakeRespError(message)

class FakeCtx(object):
    cookies = None
    resp = FakeResp()

    def __init__(self, cookies=None, user=None):
        self.cookies = cookies
        self.user = user


def test_cookie_access_good():

    fakectx = FakeCtx(cookies={'good_cookie_name': 'magic_value'})

    @needs_cookie(magic_cookies=[('good_cookie_name', 'magic_value')],
                  context=fakectx)
    def thefunction():
        return "OK"

    assert thefunction() == "OK"

def test_cookie_access_badvalue():

    fakectx = FakeCtx(cookies={'good_cookie_name': 'bad_value'})

    @needs_cookie(magic_cookies=[('good_cookie_name', 'magic_value')],
                  context=fakectx)
    def thefunction():
        return "OK"

    with pytest.raises(Exception) as e:
        thefunction()
    assert e.value.args[0] == badcookie_message

def test_cookie_access_badcookie():

    fakectx = FakeCtx(cookies={'bad_cookie_name': 'magic_value'})

    @needs_cookie(magic_cookies=[('good_cookie_name', 'magic_value')],
                  context=fakectx)
    def thefunction():
        return "OK"

    with pytest.raises(Exception) as e:
        thefunction()
    assert e.value.args[0] == badcookie_message


def test_login_access_nouser():
    fakeuser = None
    fakectx = FakeCtx(user=fakeuser)

    @needs_login(context=fakectx)
    def thefunction():
        return "OK"

    with pytest.raises(Exception) as e:
        thefunction()
    assert e.value.args[0] == notloggedin_message


def test_login_access_user():
    fakeuser = FakeUser()
    fakectx = FakeCtx(user=fakeuser)

    @needs_login(context=fakectx)
    def thefunction():
        return "OK"

    assert thefunction() == "OK"

def test_login_access_notstaff():
    fakeuser = FakeUser()
    fakectx = FakeCtx(user=fakeuser)

    @needs_login(staff=True, context=fakectx)
    def thefunction():
        return "OK"

    with pytest.raises(Exception) as e:
        thefunction()
    assert e.value.args[0] == notstaff_message

def test_login_access_staff():
    fakeuser = FakeUser(staff=True)
    fakectx = FakeCtx(user=fakeuser)

    @needs_login(staff=True, context=fakectx)
    def thefunction():
        return "OK"

    assert thefunction() == "OK"