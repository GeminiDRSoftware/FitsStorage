import configparser
import os
import socket

class FitsStorageConfig(dict):
    """
    Configuration Object for Fits Storage.

    This class reads and parses configuration files from multiple locations,
    looks for default and host-specific configurations, possibly checks
    environment variables and constructs a set of configuration parameters
    as appropriate.

    Other parts of the Fits Storage system instantiate this class and then
    query it for configuration parameters as needed.

    The configuration files are ini files, parsed by the python configparser
    module.

    Files are read as follows:
    If you pass a configfile argument, that will be the only file read. Otherwise, the following
    are read in this order. Values from later files in the list take precedence:
    * /etc/fits_storage.conf
    * ~/.fits_storage.conf
    """

    def __init__(self, configfile=None, configstring=None):
        super().__init__()

        # By default, ConfigParser treats everything as a string.
        # These lists define configuration keywords that will be converted
        # to another type automatically as they are requested
        self._bools = ['using_sqlite', 'database_debug', 'use_utc']
        self._ints = ['postgres_database_pool_size', 'postgres_database_max_overflow']

        self._readfiles(configfile=configfile, configstring=configstring)

        # Some parameters can be over-ridden by environment variables
        self._env_overrides()

        # Some values are calculated from config file values
        self._calculate_values()

    def _readfiles(self, configfile=None, configstring=None):
        # builtin is the module's internal built-in config file. It provides the default values
        _builtin = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fits_storage.conf')

        # Places to look for config files. Values from later ones take precedence
        self._configfiles = [_builtin,
                             '/etc/fits_storage.conf',
                             os.path.expanduser('~/.fits_storage.conf')]

        # This can be read back externally to see which config files were actually used.
        self.configfiles_used = []

        # We make this _private so we can point the public one at default if we don't
        # have a specific hostname section
        self._config = configparser.ConfigParser()

        # Read the config files.
        if configstring:
            self._config.read_string(configstring)
        elif configfile:
            self._config.read(configfile)
            self.configfiles_used = [configfile]
        else:
            self.configfiles_used = self._config.read(self._configfiles)

        # If the config we read has a section for this hostname, point to
        # that directly. It will inherit anything not specified there from the
        # default section. If not, point directly to the default section.
        hostname = socket.gethostname()
        if hostname in self._config.sections():
            self.config = self._config[hostname]
        else:
            self.config = self._config['DEFAULT']

    def _env_overrides(self):
        envs = {'storage_root': 'FITS_STORAGE_ROOT',
                'database_url': 'FITS_STORAGE_DB_URL'}

        for key, envvar in envs.items():
            value = os.getenv(envvar)
            if value is not None:
                self.config[key] = value

    def _calculate_values(self):
        self._calculate_database_url()
        self._calculate_using_sqlite()

    def _calculate_database_url(self):
        if self.config.get('database_url', None) is None:
            sqlite_path = os.path.join(self.config['storage_root'], 'fits_storage.db')
            self.config['database_url'] = f"sqlite:///{sqlite_path}"

    def _calculate_using_sqlite(self):
        # Config values have to be strings. It will get converted back to a bool when read
        self.config['using_sqlite'] = 'True' if self.config['database_url'].startswith('sqlite:/') else 'False'

    def __getitem__(self, key):
        if key in self._bools:
            return self.config.getboolean(key)
        if key in self._ints:
            return self.config.getint(key)
        return self.config[key]

    def __setitem__(self, key, value):
        self.config[key] = value

    def __getattr__(self, item):
        return self.__getitem__(item)