from fits_storage.orm.downloadlog import DownloadLog


class MockUsageLog:
    def __init__(self, id):
        self.id = id


def test_downloadlog():
    ul = MockUsageLog(123)
    dl = DownloadLog(ul)
    assert(dl.usagelog_id == ul.id)


def test_add_message():
    ul = MockUsageLog(123)
    dl = DownloadLog(ul)
    assert(dl.notes is None)
    dl.add_note('test')
    assert(dl.notes == 'test')
    dl.add_note('test2')
    assert(dl.notes == 'test\ntest2')
