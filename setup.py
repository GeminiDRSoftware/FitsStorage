#!/usr/bin/env python

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import subprocess
import sys
import fits_storage

class PyTest(TestCommand):
    description = 'Run unit tests using py.test'
    user_options=[('pytest-args=', 'a', "Arguments to pass to py.test")]
    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = ''

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run(self):
        import pytest
        errno = pytest.main(['tests'] + self.pytest_args.split())
        sys.exit(errno)

setup(
    name='FitsStorage',
    version=fits_storage.__version__,
    # The following is need only if publishing this under PyPI or similar
    #description = '...',
    #author = 'Paul Hirst',
    #author_email = 'phirst@gemini.edu',
    #license = 'BSD',
    tests_require=['pytest'],
    cmdclass = {'pytest': PyTest},
    packages = ['fits_storage',
                'fits_storage.cal',
                'fits_storage.orm',
                'fits_storage.utils',
                'fits_storage.web'],
    install_requires = ['psycopg2 >= 2.5',
                        'sqlalchemy >= 0.9.9',
                        'pyinotify >= 0.9',
                        'docopt >= 0.6',
                        'pyfits >= 3.2'],
    # We should use entry_points instead of scripts, but that requires
    # modifying the scripts...
    scripts = [
        'fits_storage/scripts/add_to_calcache_queue.py',
        'fits_storage/scripts/add_to_export_queue.py',
        'fits_storage/scripts/add_to_ingest_queue.py',
        'fits_storage/scripts/add_to_prevew_queue.py',
        'fits_storage/scripts/service_calcache_queue.py',
        'fits_storage/scripts/service_export_queue.py',
        'fits_storage/scripts/service_ingest_queue.py',
        'fits_storage/scripts/service_preview_queue.py',
        ]
)
