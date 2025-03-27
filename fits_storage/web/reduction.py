"""
This module generates the reduction (Processing) web page
"""
from sqlalchemy import select
from sqlalchemy.exc import MultipleResultsFound

from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header

from fits_storage.server.wsgi.context import get_context

from fits_storage.web import templating

@templating.templated('reduction.html')
def reduction(thing):
    """
    Generate a reduction (ie processing) page for 'thing'. 'thing' can be
    a filename or a header_id
    """
    session = get_context().session

    # We need to find the header for thing.
    diskfile = None
    try:
        header_id = int(thing)
        # If that worked, we got a header_id directly.
        # session.get() returns None if no can.
        header = session.get(Header, header_id)
    except ValueError:
        # We (probably) got  filename, look up the header id.
        try:
            # SQLALchemy 2.0 syntax. This will return None if there are none
            header = session.execute(
                select(Header).join(DiskFile).join(File)
                .where(DiskFile.canonical == True)
                .where(File.name == thing))\
                .scalar_one_or_none()
        except MultipleResultsFound:
            header = None

    if header is None:
        # Not found template dict
        return {'filename': 'Not Found'}

    # Find the header to get the reduction info
    reduction_orms = header.reduction_orms
    processing_tag_orms = header.processing_tag_orms

    # We use string value "None" to indicate not found to the template
    if len(reduction_orms) != 1:
        reduction_orm = "None"
    else:
        reduction_orm = reduction_orms[0]

    if len(processing_tag_orms) != 1:
        processing_tag_orm = "None"
    else:
        processing_tag_orm = processing_tag_orms[0]

    # Build the dict to pass to the template engine
    ret_dict = {'reduction': reduction_orm,
                'processing_tag': processing_tag_orm,
                'filename': header.diskfile.file.name,
                }

    return ret_dict
