import pytest

from fits_storage.web.user import needs_login, needs_cookie

from fits_storage.config import get_config
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


def get_cookietest_config():
    # Note we have to pick a real cookie name here, as those are listed in
    # fits_storage.conf _lists.
    configtext = """
            [DEFAULT]
            is_server = True
            gemini_api_authorization = ['good_value1', 'good_value2']
            """
    return get_config(reload=True, builtinonly=True, configstring=configtext)


def test_cookie_access_good():

    fakectx = FakeCtx(cookies={'gemini_api_authorization': 'good_value1'})

    fsc = get_cookietest_config()

    @needs_cookie('gemini_api_authorization', context=fakectx,
                  fsconfig=fsc)
    def thefunction():
        return "OK"

    assert thefunction() == "OK"

    fakectx = FakeCtx(cookies={'gemini_api_authorization': 'good_value2'})

    @needs_cookie('gemini_api_authorization', context=fakectx,
                  fsconfig=get_cookietest_config())
    def thefunction():
        return "OK"

    assert thefunction() == "OK"


def test_cookie_access_badvalue():

    fakectx = FakeCtx(cookies={'gemini_api_authorization': 'bad_value'})

    @needs_cookie(magic_cookie='gemini_api_authorization',
                  context=fakectx, fsconfig=get_cookietest_config())
    def thefunction():
        return "OK"

    with pytest.raises(Exception) as e:
        thefunction()
    assert e.value.args[0] == badcookie_message


def test_cookie_access_badcookie():

    fakectx = FakeCtx(cookies={'bad_cookie_name': 'magic_value'})

    @needs_cookie(magic_cookie='gemini_api_authorization',
                  context=fakectx, fsconfig=get_cookietest_config())
    def thefunction():
        return "OK"

    with pytest.raises(Exception) as e:
        thefunction()
    assert e.value.args[0] == badcookie_message


def test_login_access_nouser():
    fakeuser = None
    fakectx = FakeCtx(user=fakeuser)

    @needs_login(context=fakectx, fsconfig=get_config(builtinonly=True))
    def thefunction():
        return "OK"

    with pytest.raises(Exception) as e:
        thefunction()
    assert e.value.args[0] == notloggedin_message


def test_login_access_user():
    fakeuser = FakeUser()
    fakectx = FakeCtx(user=fakeuser)

    @needs_login(context=fakectx, fsconfig=get_config(builtinonly=True))
    def thefunction():
        return "OK"

    assert thefunction() == "OK"


def test_login_access_notstaff():
    fakeuser = FakeUser()
    fakectx = FakeCtx(user=fakeuser)

    @needs_login(staff=True, context=fakectx,
                 fsconfig=get_config(builtinonly=True))
    def thefunction():
        return "OK"

    with pytest.raises(Exception) as e:
        thefunction()
    assert e.value.args[0] == notstaff_message


def test_login_access_staff():
    fakeuser = FakeUser(staff=True)
    fakectx = FakeCtx(user=fakeuser)

    @needs_login(staff=True, context=fakectx,
                 fsconfig=get_config(builtinonly=True))
    def thefunction():
        return "OK"

    assert thefunction() == "OK"


def test_login_bypass_false():
    configtext = """
        [DEFAULT]
        fits_system_status: development
        development_bypass_auth: False
        """
    fakeuser = None
    fakectx = FakeCtx(user=fakeuser)

    fsc = get_config(reload=True, configstring=configtext)
    assert fsc.fits_system_status == 'development'
    assert fsc.development_bypass_auth is False

    @needs_login(context=fakectx, fsconfig=fsc)
    def thefunction():
        return "OK"

    with pytest.raises(Exception) as e:
        thefunction()
    assert e.value.args[0] == notloggedin_message


def test_login_bypass_true():
    configtext = """
        [DEFAULT]
        fits_system_status: development
        development_bypass_auth: True
        """
    fsc = get_config(reload=True, configstring=configtext)
    assert fsc.fits_system_status == "development"
    assert fsc.development_bypass_auth is True

    fakeuser = None
    fakectx = FakeCtx(user=fakeuser)

    @needs_login(context=fakectx, fsconfig=fsc)
    def thefunction():
        return "OK"

    assert thefunction() == 'OK'


def test_cookie_bypass_true():
    configtext = """
        [DEFAULT]
        fits_system_status: development
        development_bypass_auth: True
        """
    fsc = get_config(reload=True, configstring=configtext)
    assert fsc.fits_system_status == "development"
    assert fsc.development_bypass_auth is True

    fakectx = FakeCtx()

    @needs_cookie(magic_cookie='whatevah', context=fakectx, fsconfig=fsc)
    def thefunction():
        return "OK"

    assert thefunction() == 'OK'
