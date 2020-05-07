# Provides functionality to get reports on publications by their bibcode

from . import templating

from ..fits_storage_config import fits_system_status

from ..orm.publication import Publication

from ..utils.web import get_context, Return


@templating.templated("publication_ads.txt", content_type='text/plain')
def publication_ads(bibcode=None):
    ctx = get_context()
    resp = ctx.resp
    session = ctx.session

    if bibcode is None:
        # OK, they must have fed us garbage
        resp.content_type = "text/plain"
        resp.client_error(Return.HTTP_NOT_FOUND, "Need to provide a bibcode")

    if bibcode is not None and bibcode.startswith('bibcode='):
        bibcode = bibcode[8:]
    query = session.query(Publication).filter(Publication.bibcode == bibcode)
    publication = query.first()
    if publication is None:
        resp.content_type = "text/plain"
        resp.client_error(Return.HTTP_NOT_FOUND, "No rows found")
    return dict(
        bibcode = publication.bibcode,
        author = publication.author,
        journal = publication.journal,
        year = publication.year,
        title = publication.title
    )


@templating.templated("list_publications.txt", content_type='text/plain')
def list_publications():
    ctx = get_context()
    resp = ctx.resp
    session = ctx.session

    publications = list()
    query = session.query(Publication).order_by(Publication.bibcode) # .filter(Publication.bibcode == bibcode)
    for publication in query:
        publications.append(dict(
            bibcode = publication.bibcode,
        ))
    
    # TODO this could be cleaner, or some sort of config setting for host url
    if fits_system_status == 'development':
        url = 'https://fits/'
    else:
        url = 'https://archive.gemini.edu/'

    return dict(
        url=url,
        publications=publications
    )
