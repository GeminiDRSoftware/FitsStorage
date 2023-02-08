#!/usr/bin/env python

# This is almost certainly broken and will need fixing after we
# complete the merge of FitsStorageDB aka Gemini_Obs_DB and Gemini_CalMgr.
# This is a really rough merge and the wave function for the final
# package and module names has not yet collapsed.

from setuptools import setup
from fits_storage_core import __version__

setup(
    name='FitsStorageCore',
    version=__version__,
    # The following is need only if publishing this under PyPI or similar
    #description = '...',
    #author = 'Paul Hirst',
    #author_email = 'phirst@gemini.edu',
    license = 'License :: OSI Approved :: BSD License',
    packages = ['fits_storage_core',
                'fits_storage_core.orm',
                'fits_storage_cal']
    install_requires = ['sqlalchemy >= 0.9.9', ]  # , 'pyfits', 'numpy']
    scripts = ['fits_storage_cal/scripts/calcheck']
)