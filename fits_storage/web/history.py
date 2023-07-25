"""
This module generates the history and provenance web page
"""
from sqlalchemy import select
from sqlalchemy.exc import MultipleResultsFound

from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile

from fits_storage.server.wsgi.context import get_context

from fits_storage.web import templating

@templating.templated('history.html')
def history(thing):
    """
    Generate a provenance and history page for 'thing'. 'thing' can be
    a filename or a diskfile_id
    """
    session = get_context().session

    # We need to find the diskfile for thing.
    diskfile = None
    try:
        diskfile_id = int(thing)
        # If that worked, we got a diskfile_id directly.
        # session.get() returns None if no can.
        diskfile = session.get(DiskFile, diskfile_id)
    except ValueError:
        # We (probably) got  filename, look up the diskfile id.
        try:
            # SQLALchemy 2.0 syntax. This will return None if there are none
            diskfile = session.execute(
                select(DiskFile).join(File)
                .where(DiskFile.canonical == True)
                .where(File.name == thing))\
                .scalar_one_or_none()
        except MultipleResultsFound:
            diskfile = None

    if diskfile is None:
        # Not found template dict
        return {'filename': 'Not Found'}

    history_list = diskfile.history
    provenance_list = diskfile.provenance

    # Build the dict to pass to the template engine
    ret_dict = {'history_list': history_list,
                'provenance_list': provenance_list,
                'history_len': len(history_list),
                'provenance_len': len(provenance_list),
                'filename': diskfile.file.name,
                }

    return ret_dict
