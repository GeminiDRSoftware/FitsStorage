#!/usr/bin/env python

"""
   There are certain operations where the web server would need to access
   and modify files. For this, we'd need the server to have permissions over
   the files, which we don't want, because of the potential security
   problems.

   Here we have instead a backend API server that isolates the user interface
   from the actual file operations

"""

#######################################################################################
#
#   Generic stuff, mainly to control the routing and error responses
#
import datetime
import json
import os
import traceback
import sys

import wsgiref.simple_server
from wsgiref.validate import validator
from wsgiref.util import request_uri, application_uri, shift_path_info

from fits_storage.utils.api import json_api_call, WSGIError, NewCardsIncluded
from fits_storage.utils.api import METHOD_NOT_ALLOWED, NOT_FOUND

from fits_storage.logger import logger, setdebug, setdemon
import logging
import argparse
parser = argparse.ArgumentParser("Backend server for the Apache frontend. Performs operations as a separate user")
parser.add_argument("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_argument("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")
options = parser.parse_args()

# NOTE: Maybe we want this to be a startup option
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
logger.info("*********  api_backend.py - starting up at %s" % datetime.datetime.now())

from fits_storage.fits_storage_config import api_backend_location
from fits_storage.orm import session_scope

def get_route(environ, routes):
    req_meth  = environ['REQUEST_METHOD']
    path_info = environ['PATH_INFO']
    try:
        by_method = routes[req_meth]
    except KeyError:
        raise WSGIError("'{}' is not a valid method for this server".format(req_meth), status=METHOD_NOT_ALLOWED)

    try:
        # TODO: This needs to be expanded to recognize variable paths, mainly for GET
        #       but also for PUT/DELETE
        return by_method[path_info]
    except KeyError:
        raise WSGIError("'{}' not a valid query for method '{}'".format(path_info, req_meth), status=NOT_FOUND)

#######################################################################################
#
#   API Code
#

import pyfits as pf
from fits_storage.utils.fitseditor import compare_cards, modify_multiple_cards, all_cards_exist

def fits_is_unchanged(path, new_values):
    return all(compare_cards(path, new_values, ext=0))

def fits_apply_changes(path, changes, reject_new):
    if fits_is_unchanged(path, changes):
        logger.info("fits_apply_changes: %s [NOT MODIFIED]", path)
        return False

    if reject_new and not all_cards_exist(path, changes):
        raise WSGIError("Operational error", error_object=NewCardsIncluded())

    modify_multiple_cards(path, changes, ext=0)
    logger.info("fits_apply_changes: %s [%s]", path, str(changes))
    return True

@json_api_call(logger)
def set_image_metadata(path, changes, reject_new=False):
    try:
        return fits_apply_changes(path, changes, reject_new)
    except (pf.VerifyError, IOError) as e:
        logger.debug("Error: %s", str(e))
        raise WSGIError("There were problems when opening/modifying the file: {}".format(path))

#######################################################################################

from fits_storage.orm.fileuploadlog import FileUploadLog, FileUploadWrapper
from fits_storage.orm.miscfile import is_miscfile, miscfile_meta, miscfile_meta_path
from fits_storage.utils.ingestqueue import IngestQueueUtil
from fits_storage.fits_storage_config import storage_root, upload_staging_path, processed_cals_path, using_s3
import shutil
if using_s3:
    from fits_storage.utils.aws_s3 import get_helper

@json_api_call(logger)
def ingest_upload(filename, fileuploadlog_id=None, processed_cal=False):
    logger.info("ingest_upload: filename: %s, fileuploadlog_id: %s, processed_cal: %s", filename, fileuploadlog_id, processed_cal)
    path = processed_cals_path if processed_cal else ''
    fileuploadlog = FileUploadWrapper()

    with session_scope() as session:
        if fileuploadlog_id is not None:
            fileuploadlog.set_wrapped(session.query(FileUploadLog).get(fileuploadlog_id))

        # Move the file to its appropriate location in storage_root/path or S3
        # Construct the full path names and move the file into place
        src = os.path.join(upload_staging_path, filename)
        dst = os.path.join(path, filename)
        fileuploadlog.destination = dst

        it_is_misc = is_miscfile(src)
        try:
            if it_is_misc:
                misc_meta = miscfile_meta(src, urlencode=True)
        except Exception as e:
            print(sys.exc_info()[1])
            print('\n'.join(traceback.format_tb(sys.exc_info()[2])))
            raise e

        if using_s3:
            logger.debug("Copying to S3")
            # Copy to S3
            try:
                s3 = get_helper()
                with fileuploadlog:
                    extra_meta = {}
                    if it_is_misc:
                        extra_meta = misc_meta
                    fileuploadlog.s3_ok = s3.upload_file(dst, src, extra_meta)
                os.unlink(src)
                if it_is_misc:
                    os.unlink(miscfile_meta_path(src))
                logger.debug("Copy to S3 appeared to work")
            except Exception as e:
                string = traceback.format_tb(sys.exc_info()[2])
                string = "".join(string)
                if fileuploadlog.ful:
                    fileuploadlog.add_note("Exception during S3 upload, see log file")
                    session.flush()
                logger.error("Exception during S3 upload: %s : %s... %s" % (sys.exc_info()[0], sys.exc_info()[1], string))
                raise WSGIError("Error when trying to move a file into S3: {!r}".format(str(e)))

        else:
            try:
                dst = os.path.join(storage_root, dst)
                logger.debug("Moving %s to %s" % (src, dst))
                # We can't use os.rename as that keeps the old permissions and ownership, which we specifically want to avoid
                # Instead, we copy the file and the remove it
                shutil.copy(src, dst)
                os.unlink(src)
                fileuploadlog.file_ok = True
            except (IOError, OSError) as e:
                raise WSGIError("Error when trying to move a file into the storage: {!r}".format(str(e)))

        logger.info("Queueing for Ingest: %s" % dst)
        iq_id = IngestQueueUtil(session, logger).add_to_queue(filename, path)

        fileuploadlog.ingestqueue_id = iq_id

    return True

#######################################################################################

@json_api_call(logger)
def log_message(message, args=(), level=logging.INFO):
    try:
        args = tuple(args)
    except TypeError as e:
        raise WSGIError("Invalid argument for 'args'. Must be an iterable")

    logger.log(level, message, *args)

    return True

#######################################################################################
#
#   Routes and application entry point
#

routes = {
    'POST': {
        '/set_image_metadata': set_image_metadata,
        '/ingest_upload':      ingest_upload,
        '/log':                log_message,
    }
}

def app(environ, start_response):
    try:
        route = get_route(environ, routes)
        return route(environ, start_response)
    except WSGIError as e:
        logger.error("[%s] %s", e.status, e.message)
        return e.response(environ, start_response)

class LoggerWSGIRequestHandler(wsgiref.simple_server.WSGIRequestHandler):
    l = logger

    def _log_message(self, level, format, *args):
        self.l.log(level, "%s - - [%s] " + format, self.client_address[0],
                                                   self.log_date_time_string(),
                                                   *args)

    def log_error(self, format, *args):
        "Reimplementation of BaseHTTPServer.log_error"

        self._log_message(logging.ERROR, format, *args)

    def log_message(self, format, *args):
        """Reimplementation of BaseHTTPServer.log_message to use a logger
           instead of sys.stderr"""

        self._log_message(logging.INFO, format, *args)


# Provide a basic WSGI server, in case we're testing or don't need any fancy
# container...
if __name__ == '__main__':
    try:
        server, port = api_backend_location.split(':')
    except ValueError:
        server = api_backend_location
        port   = '8000'

    try:
        logger.info("Server is at %s:%s", server, port)
        httpd = wsgiref.simple_server.make_server(server, int(port), app, handler_class=LoggerWSGIRequestHandler)
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nExiting after Ctrl-c")
