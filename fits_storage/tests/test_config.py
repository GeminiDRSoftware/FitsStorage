from fits_storage_core.config import FitsStorageConfig, get_config


def test_default_database_url():
    fsc = get_config()
    assert fsc.database_url == 'sqlite:///:memory:'


def test_default_using_sqlite():
    fsc = get_config()
    assert fsc.using_sqlite is True
