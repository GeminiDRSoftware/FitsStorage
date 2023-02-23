from fits_storage.scripts.YouGotDataEmail import get_and_fix_emails


def test_none_email():
    emails = list(get_and_fix_emails(None))
    assert(emails is not None)
    assert(len(emails) == 0)


def test_empty_email():
    emails = get_and_fix_emails("")
    assert(emails is not None)
    assert(len(emails) == 0)


def test_whitespace_email():
    emails = get_and_fix_emails("  ")
    assert(emails is not None)
    assert(len(emails) == 0)


def test_simple_email():
    emails = get_and_fix_emails(" foo@bar.com ")
    assert(emails is not None)
    assert(len(emails) == 1)
    assert(emails[0] == "foo@bar.com")


def test_comma_emails():
    emails = get_and_fix_emails(" foo1@bar.com, foo2@bar.com ")
    assert(emails is not None)
    assert(len(emails) == 2)
    assert(emails[0] == "foo1@bar.com")
    assert(emails[1] == "foo2@bar.com")


def test_space_emails():
    emails = get_and_fix_emails(" foo1@bar.com foo2@bar.com ")
    assert(emails is not None)
    assert(len(emails) == 2)
    assert(emails[0] == "foo1@bar.com")
    assert(emails[1] == "foo2@bar.com")


def test_comma_space_emails():
    emails = get_and_fix_emails(" foo1@bar.com, foo2@bar.com foo3@bar.com")
    assert(emails is not None)
    assert(len(emails) == 3)
    assert(emails[0] == "foo1@bar.com")
    assert(emails[1] == "foo2@bar.com")
    assert(emails[2] == "foo3@bar.com")


def test_name_emails():
    emails = get_and_fix_emails(" foo1@bar.com, Testy McTestFace foo2@bar.com")
    assert(emails is not None)
    assert(len(emails) == 2)
    assert(emails[0] == "foo1@bar.com")
    assert(emails[1] == "Testy McTestFace foo2@bar.com")
