# Provides functionality to get reports on publications by their bibcode

from ..orm.publication import Publication

from ..utils.web import get_context, Return


def publication_ads(bibcode):
    ctx = get_context()
    resp = ctx.resp
    session = ctx.session

    if bibcode is None:
        # OK, they must have fed us garbage
        resp.content_type = "text/plain"
        resp.client_error(Return.HTTP_NOT_FOUND, "Need to provide a bibcode")

    query = session.query(Publication).filter(Publication.bibcode == bibcode)
    publication = query.one()

    resp.content_type = "text/plain"
    resp.append(publication.publication_ads())
