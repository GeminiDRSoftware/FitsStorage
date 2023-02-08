from gemini_obs_db.utils.file_parser import FileParser


def test_try_or_none():
    fp = FileParser()

    def throw_type_error():
        raise TypeError()
    assert(fp._try_or_none(throw_type_error, "pytest") is None)
