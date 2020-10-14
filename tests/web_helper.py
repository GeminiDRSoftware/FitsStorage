from fits_storage.utils.web import adapter


class MockTarfile(object):
    def __init__(self, tarfilename, mode):
        self.tarfilename = tarfilename
        self.mode = mode

    def __enter__(self):
        return self

    def __exit__(self, typ, value, tb):
        pass

    def add(self, fullpath, filename):
        pass

    def addfile(self, tarinfo, bytes):
        pass


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
        self.json_list = None
        self.json_indent = None

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

    def send_json(self, thelist, indent=4):
        self.json_list = thelist
        self.json_indent = indent

    def tarfile(self, tarfilename, mode):
        return MockTarfile(tarfilename, mode)

    def respond(self, fn):
        self.stuff = 'respond called'


class MockRequest(adapter.Request):
    def __init__(self, session, *, form_data=None):
        super().__init__(session)
        self.form_data = form_data
        self.header = {'User-Agent': 'Flagon 1.0'}

    def get_form_data(self):
        return self.form_data

    def __getattr__(self, item):
        if item == 'User-Agent':
            return 'Flagon 1.0'
        return super().__getattr__(item)

    def env(self):
        pass

    def get_header_value(self, header_name):
        pass

    def input(self):
        pass

    def log(self):
        pass

    def raw_data(self):
        pass

    def contains_header(self, name):
        return name in self.header.keys()


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
    def __init__(self, session, *, method='GET', form_data=dict(), raw_data=None, is_staffer=False,
                 usagelog=MockUsageLog()):
        self.session = session
        self.env = MockEnv(method=method)
        self.resp = MockResponse()
        self.usagelog = usagelog
        self.user = MockUser()
        # self.req = {'User-Agent': 'Flagon 1.0'}
        self.req = MockRequest(session, form_data=form_data)
        self.got_magic = False
        self.is_staffer = is_staffer
        self.form_data = form_data
        self.raw_data = raw_data

    def get_form_data(self, *args, **kwargs):
        return self.form_data

    def json(self):
        return '{}'
