# Provides functionality to get reports on publications by their bibcode

from . import templating
from .templating import SkipTemplateError

from fits_storage.server.orm.publication import Publication

from fits_storage.server.wsgi.context import get_context
from fits_storage.server.wsgi.returnobj import Return


@templating.templated("publication_ads.txt", content_type='text/plain')
def publication_ads(bibcode=None):
    ctx = get_context()
    resp = ctx.resp
    session = ctx.session

    if bibcode is None:
        # OK, they must have fed us garbage
        resp.status = Return.HTTP_NOT_FOUND
        resp.content_type = "text/plain"
        resp.client_error(Return.HTTP_NOT_FOUND, "Need to provide a bibcode")
        raise SkipTemplateError(Return.HTTP_NOT_FOUND)

    if bibcode is not None and bibcode.startswith('bibcode='):
        bibcode = bibcode[8:]
    query = session.query(Publication).filter(Publication.bibcode == bibcode)
    publication = query.first()
    if publication is None:
        resp.status = Return.HTTP_NOT_FOUND
        resp.content_type = "text/plain"
        resp.client_error(Return.HTTP_NOT_FOUND, "No rows found")
        raise SkipTemplateError(Return.HTTP_NOT_FOUND)
    else:
        return dict(
            bibcode=publication.bibcode,
            author=publication.author,
            journal=publication.journal,
            year=publication.year,
            title=publication.title
        )


@templating.templated("list_publications.txt", content_type='text/plain')
def list_publications():
    ctx = get_context()
    session = ctx.session

    publications = list()
    query = session.query(Publication).order_by(Publication.bibcode)
    for publication in query:
        publications.append(dict(
            bibcode=publication.bibcode,
        ))
    
    url = 'https://archive.gemini.edu/'

    return dict(
        url=url,
        publications=publications
    )
