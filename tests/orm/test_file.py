from gemini_obs_db.file import File


def test_file():
    f = File("filename.fits")
    assert(f.name == "filename.fits")


def test_file_bz2():
    f = File("filename.fits.bz2")
    assert(f.name == "filename.fits")


def test_file_repr():
    f = File("filename.fits")
    f.id = 123
    assert(f.__repr__() == "<File('%s', '%s')>" % (f.id, f.name))
