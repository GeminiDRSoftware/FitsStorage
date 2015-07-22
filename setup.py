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
    tests_require=['pytest'],
    cmdclass = {'pytest': PyTest}
)
