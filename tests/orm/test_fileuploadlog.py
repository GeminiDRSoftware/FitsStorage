from fits_storage.orm.fileuploadlog import FileUploadLog


class MockUsageLog:
    def __init__(self, id):
        self.id = id


def test_filedownloadlog():
    ul = MockUsageLog(123)
    fdl = FileUploadLog(ul)
    assert(fdl.usagelog_id == 123)


def test_notes():
    ul = MockUsageLog(123)
    fdl = FileUploadLog(ul)
    assert(fdl.notes is None)
    fdl.add_note('note1')
    assert(fdl.notes == 'note1')
    fdl.add_note('note2')
    assert(fdl.notes == 'note1\nnote2')
