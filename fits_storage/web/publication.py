# Provides functionality to get reports on publications by their bibcode

from . import templating

from ..orm.publication import Publication

from ..utils.web import get_context, Return


@templating.templated("publication_ads.txt", content_type='text/plain')
def publication_ads(bibcode=None):
    ctx = get_context()
    resp = ctx.resp
    session = ctx.session

    print("In publication_ads")

    # if bibcode is None:
    #     # OK, they must have fed us garbage
    #     resp.content_type = "text/plain"
    #     resp.client_error(Return.HTTP_NOT_FOUND, "Need to provide a bibcode")

    if bibcode is not None and bibcode.startswith('bibcode='):
        bibcode = bibcode[8:]
    print("Doing query, bibcode is %s" % bibcode)
    query = session.query(Publication) # .filter(Publication.bibcode == bibcode)
    publication = query.first()
    print("Did query for one row")
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

    resp.content_type = "text/plain"
    resp.append(publication.publication_ads())
