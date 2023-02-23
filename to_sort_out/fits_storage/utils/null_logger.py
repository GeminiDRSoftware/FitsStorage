class EmptyLogger(object):
    """Dummy logger object. We won't use the NullHandler from logger because we really don't want
       to import the logging module from this one. It would affect the web server and maybe
       other servers"""
    def __getattr__(self, attr):
        return self

    def __call__(self, *args, **kw):
        pass

