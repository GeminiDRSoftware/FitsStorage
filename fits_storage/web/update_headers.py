from sqlalchemy.exc import NoResultFound, MultipleResultsFound

from fits_storage.server.wsgi.context import get_context
from fits_storage.server.wsgi.returnobj import Return

from .user import needs_cookie
from fits_storage.logger_dummy import DummyLogger

from fits_storage.queues.queue.fileopsqueue import FileopsQueue, FileOpsRequest

from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header

from fits_storage.config import get_config
fsc = get_config()


def error_response(message, id=None):
    response = {'result': False, 'error': message}
    if id is not None:
        response['id'] = id

    return response


def check_exists(session, fn=None, dl=None):
    # Check if a filename or datalabel exists, is present, and is unique
    # - ie that we are likely to be able to do a header update on it.
    # Returns: 0 if does not exist, 1 if does exist, 2 if multiple files exist.

    if fn is not None:
        filequery = session.query(File).\
            filter(File.name == fn.removesuffix('.bz2'))
    elif dl is not None:
        filequery = session.query(File).join(DiskFile).join(Header)\
            .filter(Header.data_label == dl).filter(DiskFile.canonical == True)
    else:
        return 0

    try:
        thefile = filequery.one()
        diskfile = session.query(DiskFile)\
            .filter(DiskFile.file_id == thefile.id) \
            .filter(DiskFile.present == True).one()
        return 1
    except NoResultFound:
        return 0
    except MultipleResultsFound:
        return 2


@needs_cookie(magic_cookie='gemini_api_authorization', content_type='json')
def update_headers():
    ctx = get_context()
    resp = ctx.resp
    resp.content_type = 'application/json'

    try:
        message = ctx.json

        # Add json request message to usagelog
        ctx.usagelog.add_note("Request: %s" % str(message))

        # TODO - get rid of "new" format once fixHead.py has been updated
        # or replaced. The ODB uses "old" format.

        # The old format for the request was the list now in "request".
        # This will provide compatibility for both formats
        if isinstance(message, dict):
            # New format
            payload = message['request']
        else:
            # Assume old format
            payload = message

        # Instantiate a FileOps Queue helper object
        fq = FileopsQueue(session=ctx.session, logger=DummyLogger())

        # Loop through all the requests in the payload and  add them to the
        # fileops queue. We queue these as response_required = False as the
        # ingest is always asynchronous anyway, so there's little value to
        # the caller in getting a confirmation that the fileops update_header
        # succeeded as they're going to have to basically poll to check it
        # ingested or just assume success anyway. We don't need to add the
        # file to the ingest queue here, the fileops update_headers method
        # takes care of that.

        results = []
        for update in payload:
            fn = update.get('filename')
            dl = update.get('data_label')
            values = update.get('values')
            if not isinstance(values, dict):
                results.append(
                    error_response('This looks like a malformed request. '
                                   'values should be a dictionary'))
                continue
            args = {}
            if fn is not None:
                args['filename']= fn
            elif dl is not None:
                args['data_label'] = dl
            else:
                results.append(
                    error_response('No filename or data_label given'))
                continue
            if fn is not None and dl is not None:
                results.append(
                    error_response('filename and data_label both given -'
                                   'this is an invalid request'))
                continue

            for key in values:
                args[key] = values[key]

            exists = check_exists(ctx.session, fn=fn, dl=dl)
            if exists == 0:
                results.append(
                    error_response('No present file found for filename or '
                                   'datalabel', id=fn or dl))
                continue
            elif exists == 2:
                results.append(
                    error_response('Multiple files found for filename or '
                                   'datalabel - request is ambiguous',
                                   id=fn or dl))
                continue


            fqrq = FileOpsRequest(request='update_headers', args=args)
            fq.add(fqrq, filename=fn, response_required=False)
            response = {'result': True, 'id': fn if fn else dl}
            results.append(response)

        # Add response to usagelog notes
        ctx.usagelog.add_note("Response: %s" % str(results))

        resp.append_json(results)
        resp.status = Return.HTTP_OK

    except KeyError as e:
        resp.append_json([error_response(e)])
        resp.status = Return.HTTP_BAD_REQUEST
    except Exception as e:
        resp.append_json([error_response(e)])
        resp.status = Return.HTTP_INTERNAL_SERVER_ERROR
