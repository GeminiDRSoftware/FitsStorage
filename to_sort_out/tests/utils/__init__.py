class MockRequest:
    def __init__(self, filename='filename', size=4, md5='data_md5', code=200, raises_err=None):
        self.filename = filename
        self.size = size
        self.md5 = md5
        self.code = code
        self.call = 0
        md5 = self.md5
        if not isinstance(md5, str):
            md5 = md5[self.call % len(md5)]
        self.text = '[{"filename": "%s", "size": %d, "md5": "%s"}]' % (self.filename, self.size, md5)
        self.json = json.loads(self.text)
        self.status_code = 200
