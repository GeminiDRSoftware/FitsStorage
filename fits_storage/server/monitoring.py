"""
This module contains instrument monitoring definitions, eg the keywords that
we capture from each recipe.

By convention, the data checking recipes write output files with the recipe
name as the outpur suffix - ie checkBias1 writes _checkBias1.fits files.
"""

# These tell us the keywords that are recorded by each recipe
recipe_keywords = {
    'checkBiasOSCO': ['OVERSCAN', 'OVERRMS', 'OVERRDNS',
                      'OSCOMEAN', 'OSCOSTDV', 'OSCOMED'],
    'checkBiasBICO': ['BICOMEAN', 'BICOSTDV', 'BICOMED'],
    'checkBiasSTCO': ['STCOMEAN', 'STCOSTDV', 'STCOMED'],
    'checkFlatCounts': ['FLATMEAN', 'FLATSTDV', 'FLATMED'],
}

# These tell us the keywords to include (if they are present) in each report
# type
report_keywords = {
    'checkBias': ['OVERSCAN', 'OVERRMS', 'OVERRDNS',
                  'OSCOMEAN', 'OSCOSTDV', 'OSCOMED',
                  'BICOMEAN', 'BICOSTDV', 'BICOMED',
                  'STCOMEAN', 'STCOSTDV', 'STCOMED'],
    'checkFlat': ['FLATMEAN', 'FLATSTDV', 'FLATMED'],
}