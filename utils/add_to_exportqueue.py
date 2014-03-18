"""
This module provides various utility functions for adding to the export queue
"""
from orm.exportqueue import ExportQueue
from logger import logger
from sqlalchemy.orm.exc import ObjectDeletedError


def addto_exportqueue(session, filename, path, destination):
    """
    Adds a file to the export queue
    """
    logger.info("Adding file %s to %s to exportqueue" % (filename, destination))
    eq = ExportQueue(filename, path, destination)
    session.add(eq)
    session.commit()
    try:
        logger.debug("Added id %d for filename %s to exportqueue" % (eq.id, eq.filename))
        return eq.id
    except ObjectDeletedError:
        logger.debug("Added filename %s to exportqueue which was immediately deleted" % filename)
