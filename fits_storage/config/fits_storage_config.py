"""
fits_storage_conf module.
This module contains the FitsStorageConfig class definition
"""

import configparser
import os
import socket
from ast import literal_eval


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

    The built-in minimal config file is read first by default. This can be
    disabled by passing builtin = False

    If you pass a configstring argument, that will be the only other
    configuration read. A configstring is a string that is treated as if it
    were the contents of a config file.

    If you pass a configfile argument, that file will be the only other
    configuration file read. If configfile is an empty string, no other
    configuration files will be read.


    Otherwise, the following are read in this order:
    * /etc/fits_storage.conf
    * ~/.fits_storage.conf

    When reading configuration values from multiple places, any values read
    later will take precedence over values read earlier.
    """

    # By default, ConfigParser treats everything as a string.
    # These lists define configuration keywords that will be converted
    # to another type automatically as they are requested
    _bools = ['using_sqlite', 'database_debug', 'use_utc', 'is_server',
              'is_archive', 'using_s3', 'using_previews', 'using_fitsverify',
              'logreports_use_materialized_view', 'ordid_enabled']
    _ints = ['postgres_database_pool_size', 'postgres_database_max_overflow',
             'defer_threshold', 'defer_delay', 'fits_open_result_limit',
             'fits_closed_result_limit']
    _lists = ['blocked_urls']

    def __init__(self, configfile=None, configstring=None, builtin=True):
        super().__init__()


        self._readfiles(configfile=configfile,
                        configstring=configstring,
                        builtin=builtin)

        # Some parameters can be over-ridden by environment variables
        self._env_overrides()

        # Some values are calculated from config file values
        self._calculate_values()

    def _readfiles(self, configfile=None, configstring=None, builtin=True):
        """
        Read in the prescribed configuration files or strings
        Parameters
        ----------
        configfile: config file to read. See class documentation
        configstring: config file to read. See class documentation
        builtin: whether to read the built-in minimal configuration file.

        Returns
        -------
        None
        """

        # builtin is the module's internal built-in config file.
        # It provides the default values
        _builtin = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                'fits_storage.conf')

        # Places to look for config files. Later ones take precedence
        self._configfiles = ['/etc/fits_storage.conf',
                             os.path.expanduser('~/.fits_storage.conf')]

        # This can be read back to see which config files were actually used.
        self.configfiles_used = []

        # We make this _private so that we can point the public one at
        # 'DEFAULT' if we don't have a specific hostname section
        self._config = configparser.ConfigParser()

        # Read the config files.
        if builtin:
            self._config.read(_builtin)
            self.configfiles_used.append(_builtin)
        if configstring is not None:
            self._config.read_string(configstring)
            self.configfiles_used.append(':passed-configstring:')
        elif configfile is not None:
            if configfile != '':
                self._config.read(configfile)
                self.configfiles_used.append(configfile)
        else:
            self.configfiles_used.extend(self._config.read(self._configfiles))

        # If the config we read has a section for this hostname, point to
        # that directly. It will inherit anything not specified there from the
        # default section. If not, point directly to the default section.
        hostname = socket.gethostname()
        if hostname in self._config.sections():
            self.config = self._config[hostname]
        else:
            self.config = self._config['DEFAULT']

    def _env_overrides(self):
        """
        Check for environment variables containing configuration values
        """
        envs = {'storage_root': 'FITS_STORAGE_ROOT',
                'database_url': 'FITS_STORAGE_DB_URL'}

        for key, envvar in envs.items():
            value = os.getenv(envvar)
            if value is not None:
                self.config[key] = value

    def _calculate_values(self):
        """
        After reading all configuration items in, calculate any additional
        vales that are automatically derived from those values read in.
        """
        self._calculate_database_url()
        self._calculate_using_sqlite()
        self._calculate_using_s3()
        self._calculate_using_fitsverify()

    def _calculate_database_url(self):
        """
        If database_url is not defined, set it to an sqlite file
        in the storage_root
        """
        if self.config.get('database_url', None) is None:
            sqlite_path = os.path.join(self.config['storage_root'],
                                       'fits_storage.db')
            self.config['database_url'] = f"sqlite:///{sqlite_path}"

    def _calculate_using_sqlite(self):
        """
        Determine from the database_url if we are using an sqlite database
        """
        # Config values have to be strings.
        # It will get converted back to a bool when read
        self.config['using_sqlite'] = 'True' if \
            self.config['database_url'].startswith('sqlite:/') else 'False'

    def _calculate_using_s3(self):
        """
        Determine if we're using AWS S3 for storage.
        This defaults to the is_archive value if not specified
        """
        if 'using_s3' not in self.config:
            self.config['using_s3'] = self.config['is_archive']

    def _calculate_using_fitsverify(self):
        """
        If not set, default to the value of is_server
        """
        if self.config['using_fitsverify'] == '' :
            self.config['using_fitsverify'] = self.config['is_server']

    def _getlist(self, key):
        """
        Parse the string from the config item and return a list. This allows
        config file entries of the form: foo = ['one', 'two', 'four']

        This uses ast.literal_eval and the only error checking is that we end
        up with a list - if not, we raise a ValueError. If there are any other
        errors, we just let the exception rise, as there's liklely little we
        can do about it and we do not want to continue with missing config
        items due to a typo in the config file.

        Parameters
        ----------
        key - the name of the config item

        Returns
        -------
        the actual list
        """
        result = literal_eval(self.config[key])
        if not isinstance(result, list):
            raise ValueError
        return result

    def __getitem__(self, key):
        if key in self._bools:
            return self.config.getboolean(key)
        if key in self._ints:
            return self.config.getint(key)
        if key in self._lists:
            return self._getlist(key)
        return self.config[key]

    def __setitem__(self, key, value):
        self.config[key] = value

    def __getattr__(self, item):
        return self.__getitem__(item)
