from fits_storage.core.orm.photstandard import PhotStandard, PhotStandardObs
from fits_storage.core.orm.footprint import Footprint

from fits_storage.server.wsgi.context import get_context

from . import templating


def list_phot_std_obs(header_id):
    """
    Returns a list of the photometric standards that should be in this header id
    """

    query = get_context().session.query(PhotStandard).join(PhotStandardObs)\
        .join(Footprint).filter(Footprint.header_id == header_id)

    for q in query:
        yield q


bands = ('u', 'v', 'g', 'r', 'i', 'z', 'y', 'j', 'h', 'k', 'lprime', 'm')
band_names = [(x if x != 'lprime' else 'l_prime') for x in bands]


def get_standard_obs(header_id):
    return dict(
        bands=[x+'_mag' for x in bands],
        band_names=band_names,
        phot_std_obs=list_phot_std_obs(header_id)
        )


@templating.templated("standards/standardobs.html")
def standardobs(header_id):
    """
    sends and html table detailing the standard stars visible in this header_id
    """

    return get_standard_obs(header_id)
