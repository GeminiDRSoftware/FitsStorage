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
    resp.set_content_type('application/json')

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

        # We need to keep track of the queue entry IDs to check the responses
        fqids = []

        # Loop through all the requests in the payload, add them to the fileops
        # queue and record the queue entry IDs.
        for args in payload:
            fqrq = FileOpsRequest(request='update_headers', args=args)
            fqe = fq.add(fqrq, response_required=True)
            fqids.append(fqe.id)

        # At this point, we basically wait for the service_fileops_queue
        # scripts to actually update the files. The queue entries may not
        # complete in order, but that doesn't matter, we just work through the
        # list regardless, the poll will return instantly if the response is
        # already waiting.
        #
        # We don't need to add the file to the ingest queue here, the fileops
        # update_headers method takes care of that.

        for id in fqids:
            foresp = fq.poll_for_response(id)
            resp.append_json(foresp.json())
        resp.status = Return.HTTP_OK

    except KeyError as e:
        resp.append_json(error_response(e))
        resp.status = Return.HTTP_BAD_REQUEST
    except Exception as e:
        resp.append_json(error_response(e))
        resp.status = Return.HTTP_INTERNAL_SERVER_ERROR

