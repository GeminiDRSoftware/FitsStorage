from fits_storage.config import get_config
from fits_storage_tests.code_tests.helpers import get_test_config

# Verify we're getting a singleton object
def test_singleton():
    get_test_config()

    a = get_config()
    b = get_config()

    assert a is b

# Test that it actually reloads and returns a different instance
def test_reload():
    get_test_config()

    a = get_config()
    b = get_config(reload=True)

    assert a is not b

# Test boolean return
def test_bool():
    get_test_config()

    a = get_config()
    b = a.using_sqlite

    assert type(b) is bool

# Test int return
def test_int():
    get_test_config()

    a = get_config()
    b = a.postgres_database_pool_size

    assert type(b) is int

# Test list return
def test_list():
    get_test_config()

    a = get_config()
    b = a.blocked_urls

    assert type(b) is list
    assert len(b) == 0

# Test got correct default
def test_default_dburl():
    get_test_config()

    a = get_config(reload=True, configfile='')
    assert a.database_url == 'sqlite:///:memory:'


configtext = """
    [DEFAULT]
    database_url: arbitary string
    """
# Test configstring and setting a value
def test_configstring():
    a = get_config(reload=True, configstring=configtext)

    assert a.database_url == 'arbitary string'

import tempfile
def test_configfile():
    with tempfile.NamedTemporaryFile(mode='w') as tf:
        tf.write(configtext)
        tf.flush()
        a = get_config(configfile=tf.name, reload=True)
        assert a.database_url == 'arbitary string'

def test_configused():
    with tempfile.NamedTemporaryFile(mode='w') as tf:
        tf.write(configtext)
        tf.flush()
        a = get_config(configfile=tf.name, reload=True)
        # Should use the builtin config file and the supplied one only
        assert len(a.configfiles_used) == 2 and \
               a.configfiles_used[1] == tf.name

def test_using_fitsverify():
    get_test_config()

    a = get_config()
    assert a.using_fitsverify is True

    configtext = """
    [DEFAULT]
    is_server = False
    """
    a = get_config(configstring=configtext, reload=True)
    assert a.using_fitsverify is False