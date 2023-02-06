from .fits_storage_config import FitsStorageConfig
__all__ = ['get_config']

_config = None

def get_config():
    global _config
    if _config is None:
        _config = FitsStorageConfig()

    return _config