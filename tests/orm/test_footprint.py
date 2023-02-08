from fits_storage.orm.footprint import Footprint


class MockHeader:
    def __init__(self, id):
        self.id = id


def test_footprint():
    fp = Footprint(MockHeader(123))
    assert(fp.header_id == 123)


def test_footprint_extension():
    fp = Footprint(MockHeader(123))
    fp.populate("extension")
    assert(fp.extension == "extension")


