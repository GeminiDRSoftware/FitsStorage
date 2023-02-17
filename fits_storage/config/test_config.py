from fits_storage.config import get_config

# Verify we're getting a singleton object
def test_singleton():
    a = get_config()
    b = get_config()

    assert a is b

# Test that it actually reloads and returns a different instance
def test_reload():
    a = get_config()
    b = get_config(reload=True)

    assert a is not b

# Test boolean return
def test_bool():
    a = get_config()
    b = a.using_sqlite

    assert type(b) is bool

# Test int return
def test_int():
    a = get_config()
    b = a.postgres_database_pool_size

    assert type(b) is int

# Test got correct default
def test_default_dburl():
    a = get_config(reload=True, configfile='')
    assert a.database_url == 'sqlite:///:memory:'

# Test configstring and setting a value
def test_configstring():
    cs = """
    [DEFAULT]
    database_url: arbitary string
    """
    b = get_config(reload=True, configstring=cs)

    assert b.database_url == 'arbitary string'