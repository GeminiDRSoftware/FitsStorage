"""
__init__.py file for fits_storage.config
This module provides housekeeping to return a singleton FitsStorageConfig
instance.
"""

from .fits_storage_config import FitsStorageConfig
__all__ = ['get_config', 'FitsStorageConfig']

_CONFIG = None


def get_config(configfile=None, configstring=None, builtin=True,
               builtinonly=False, reload=False):
    """
    Instantiates (if it doesn't already exist) a singleton FitsStorageConfig
    object, and returns it.

    We store the instance in a global in this module and simply return that
    if it already exists. This avoids re-reading the configuration files for
    each request.

    Passing reload=True will force re-instantiation of the FitsStorageConfig
    object, causing configuration files to be re-read.

    Note that if get_config() has already been called, repeat calls passing
    configfile or configstring will ALSO need to pass reload=True in order
    for those arguments to take effect.

    Parameters
    ----------
    configfile: argument passed to FitsStorageConfig
    configstring: argument passed to FitsStorageConfig
    builtin: argument passed to FitsStorageConfig
    buildinonly: argument passed to FitsStorageConfig
    reload: force reloading the configuration

    Returns
    -------
    FitsStorageConfig configuration object instance.
    """

    global _CONFIG
    if _CONFIG is None or reload is True:
        _CONFIG = FitsStorageConfig(configfile=configfile,
                                    configstring=configstring,
                                    builtin=builtin,
                                    builtinonly=builtinonly)

    return _CONFIG
