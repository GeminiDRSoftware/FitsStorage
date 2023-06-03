from fits_storage.server.wsgi.context import get_context
from fits_storage.server.wsgi.returnobj import Return

from .user import needs_login
from fits_storage.logger import DummyLogger

from fits_storage.queues.queue.fileopsqueue import FileopsQueue, FileOpsRequest

from fits_storage.config import get_config
fsc = get_config()


def error_response(message, id=None):
    response = {'result': False, 'error': message}
    if id is not None:
        response['id'] = id

    return response


@needs_login(magic_cookies=[
    ('gemini_api_authorization', fsc.magic_api_server_cookie)],
    only_magic=True, content_type='json')
def update_headers():
    ctx = get_context()
    resp = ctx.resp
    resp.content_type = 'application/json'

    try:
        message = ctx.json

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

        for update in payload:
            fn = update.get('filename')
            dl = update.get('data_label')
            values = update.get('values')
            args = {}
            if fn:
                args['filename']= fn
            elif dl:
                args['data_label'] = dl
            for key in values:
                args[key] = values[key]

            if fn is None and dl is None:
                response = {'result': False, 'value': 'No filename or datalabel given'}
                resp.append_json(response)
                resp.status = Return.HTTP_BAD_REQUEST
                return

            # TODO - check that the filename or datalable exists,
            # return a bad status if not.

            fqrq = FileOpsRequest(request='update_headers', args=args)
            fq.add(fqrq, filename=fn, response_required=False)
            response = {'result': True, 'value': True}
            response['id'] = fn if fn else dl
            resp.append_json(response)

        resp.status = Return.HTTP_OK

    except KeyError as e:
        resp.append_json(error_response(e))
        resp.status = Return.HTTP_BAD_REQUEST
    except Exception as e:
        resp.append_json(error_response(e))
        resp.status = Return.HTTP_INTERNAL_SERVER_ERROR
