
class MockUsageLog(object):
    def __init__(self):
        self.notes = list()

    def add_note(self, note):
        self.notes.append(note)


class MockClientError(object):
    def __init__(self, code, content_type=None, message=None):
        self.code = code
        self.content_type = content_type
        self.message = message


class MockResponse(object):
    def __init__(self):
        self.content_type = 'text/plain'
        self.stuff = ''
        self.error = None
        self.status = 200

    def append_iterable(self, iter):
        for i in iter:
            self.stuff += i

    def append(self, more_stuff):
        if isinstance(more_stuff, str):
            self.stuff += more_stuff
        else:
            self.stuff = "%s\n%s" % (self.stuff, more_stuff)

    def client_error(self, code, content_type=None, message=None):
        self.error = MockClientError(code, content_type=content_type, message=message)

    def set_content_type(self, content_type):
        self.content_type = content_type

    def append_json(self, json):
        self.stuff = "%s\n%s" % (self.stuff, json)


class MockEnv(object):
    def __init__(self, method='GET'):
        self.method = method
        self.remote_ip = '127.0.0.1'
        self.unparsed_uri = '/foo'
        self.uri = '/foo'


class MockUser(object):
    def __init__(self):
        self.id = 1
        self.gemini_staff = True
        self.username = 'mockuser'
        self.orcid_id = None


class MockContext(object):
    """
    Mock Web Context for unit tests.
    """
    def __init__(self, session, *, method='GET', form_data=dict(), raw_data=None, is_staffer=False):
        self.session = session
        self.env = MockEnv(method=method)
        self.resp = MockResponse()
        self.usagelog = MockUsageLog()
        self.user = MockUser()
        self.req = {'User-Agent': 'Flagon 1.0'}
        self.got_magic = False
        self.is_staffer = is_staffer
        self.form_data = form_data
        self.raw_data = raw_data

    def get_form_data(self, *args, **kwargs):
        return self.form_data

    def json(self):
        return '{}'

