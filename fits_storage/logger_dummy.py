# This needs to be in a separate module to logger so that it can be imported
# without causing the python logger to be configured

class DummyLogger(object):
    """
    A dummy object that you can treat as a logger but which does absolutely
    nothing, or optionally just prints the log message
    """
    def noop(self, *args, **kwargs):
        pass

    def justprint(self, *args, **kwargs):
        print(*args)

    def __init__(self, print=False):
        f = self.justprint if print else self.noop

        self.info = f
        self.error = f
        self.debug = f
        self.warning = f
