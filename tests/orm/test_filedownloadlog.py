from fits_storage.orm.filedownloadlog import FileDownloadLog


class MockUsageLog:
    def __init__(self, id):
        self.id = id


def test_filedownloadlog():
    ul = MockUsageLog(123)
    fdl = FileDownloadLog(ul)
    assert(fdl.usagelog_id == 123)


def test_notes():
    ul = MockUsageLog(123)
    fdl = FileDownloadLog(ul)
    assert(fdl.notes is None)
    fdl.add_note('note1')
    assert(fdl.notes == 'note1')
    fdl.add_note('note2')
    assert(fdl.notes == 'note1\nnote2')
