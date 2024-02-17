from functools import cache

from .gmos import Gmos
from .gnirs import Gnirs
from .gsaoi import Gsaoi
from .gpi import Gpi
from .niri import Niri
from .nici import Nici
from .nifs import Nifs
from .michelle import Michelle
from .f2 import F2
from .ghost import Ghost

from .ghost import GHOST_ARMS

instrument_class = {
    # Instrument: Class
    'F2': F2,
    'GMOS-N': Gmos,
    'GMOS-S': Gmos,
    'GHOST': Ghost,
    'GNIRS': Gnirs,
    'GPI': Gpi,
    'GSAOI': Gsaoi,
    'michelle': Michelle,
    'NICI': Nici,
    'NIFS': Nifs,
    'NIRI': Niri,
    }


def get_inst_rows(header, ad, logger):
    """
    This helper function generates the instance(s) of the instrument
    specific cal orm class to add to the database. Normally we only add
    one row per header, but ghost adds multiple rows here (one for each
    "arm"). We always return a list here for simplicity, even if it's a list
    of one for most instruments
    """
    inst = header.instrument
    instclass = instrument_class.get(inst)
    if instclass is None:
        return None
    logger.debug("Creating new %s entry", inst)
    if inst == 'GHOST':
        return get_ghost_rows(header, ad, logger)
    else:
        return [instclass(header, ad)]

def get_ghost_rows(header, ad, logger):
    """
    Essentially, a ghost specific version of get_inst_rows. We use the
    AdCache class defined below to cache the results from descriptor calls
    as they can be slow and we're going to call them several times and extract
    the value for each arm from different calls.
    """
    results = []
    adc = AdCache(ad)
    adarm = adc.arm()
    if isinstance(adarm, str):
        arms = [adarm]
    elif adarm is None:
        # This causes issues for ghost files that do not have all 3 arms.
        # arms = GHOST_ARMS
        arms = adc.exposure_time().keys()
    else:
        logger.error("Bad return type for ad.arm()")
        arms = []
    for arm in arms:
        logger.debug("Creating Ghost instance for %s arm", arm)
        results.append(Ghost(header, adc, arm))
    return results


class AdCache:
    """
    Evaluating descriptors on ghost ad instances can be expensive as
    astrodata has to trawl through many FITS extensions to do so.
    When it does that on a ghost bundle, it returns a dictionary with
    values for each arm. In this module we want the values for several
    descripors for one arm, and then later for another arm, and so on. To
    avoid evaluating the descriptors multiple times, we use this cache-like
    object.
    """
    def __init__(self, ad):
        self.ad = ad
        self.tags = ad.tags
        for desc in ad.descriptors:
            setattr(self, desc, cache(getattr(ad, desc)))
