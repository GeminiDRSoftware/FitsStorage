import datetime


class MockHeader(object):
    def __init__(self, ut_datetime=None, telescope='Gemini-North', instrument='GMOS-N'):
        self.telescope = telescope
        self.instrument = instrument
        if ut_datetime is None:
            self.ut_datetime = datetime.datetime.utcnow()
        else:
            self.ut_datetime = ut_datetime
