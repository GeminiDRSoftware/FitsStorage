"""
This is the Fits Storage Web Summary module. It provides the functions
which query the database and generate html for the web header
summaries.
"""
from ..orm import sessionfactory, session_scope
from mod_python import apache

from ..orm.photstandard import PhotStandard, PhotStandardObs
from ..orm.footprint import Footprint

from . import templating

def list_phot_std_obs(session, header_id):
    """
    Returns a list of the photometric standards that should be in this header id
    """

    query = (
        session.query(PhotStandard).join(PhotStandardObs).join(Footprint)
            .filter(Footprint.header_id == header_id)
        )

    for q in query:
        yield q

bands = ('u', 'v', 'g', 'r', 'i', 'z', 'y', 'j', 'h', 'k', 'lprime', 'm')
band_names = [(x if x != 'lprime' else 'l_prime') for x in bands]

def get_standard_obs(session, req, header_id):
    return dict(
        bands = [x+'_mag' for x in bands],
        band_names = band_names,
        phot_std_obs = list_phot_std_obs(session, header_id)
        )

@templating.templated("standards/standardobs.html", with_session=True)
def standardobs(session, req, header_id):
    """
    sends and html table detailing the standard stars visisble in this header_id
    """

    return get_standard_obs(session, req, header_id)

@templating.templated("standards/standardobs.xml", content_type='text/xml', with_session=True)
def xmlstandardobs(req, header_id):
    """
    Writes xml fragment defining the standards visible in this header_id
    """

    return get_standard_obs(session, req, header_id)
