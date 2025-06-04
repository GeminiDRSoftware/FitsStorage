"""
This module contains instrument monitoring definitions, eg the keywords that
we capture from each recipe.

By convention, the data checking recipes write output files with the recipe
name as the outpur suffix - ie checkBias1 writes _checkBias1.fits files.
"""

# These tell us the keywords that are recorded by each recipe
recipe_keywords = {
    'checkBias1': ['OVERSCAN', 'OVERRMS', 'OSCOMEAN', 'OSCOSTDV', 'OSCOMED'],
    'checkBias2': ['BICOMEAN', 'BICOSTDV', 'BICOMED'],
    'checkFlat1': ['FLATMEAN', 'FLATSTDV', 'FLATMED'],
}

# These tell us the keywords to include (if they are present) in each report
# type
report_keywords = {
    'checkBias': ['OVERSCAN', 'OVERRMS', 'OSCOMEAN', 'OSCOSTDV', 'OSCOMED',
                  'BICOMEAN', 'BICOSTDV', 'BICOMED'],
    'checkFlat': ['FLATMEAN', 'FLATSTDV', 'FLATMED'],
}