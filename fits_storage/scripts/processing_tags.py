#! /usr/bin/env python3

import datetime
from sqlalchemy.exc import NoResultFound, MultipleResultsFound

from fits_storage.logger import logger, setdebug, setdemon

from fits_storage.db import sessionfactory

from fits_storage.server.orm.processingtag import ProcessingTag

from argparse import ArgumentParser

from fits_storage.config import get_config
fsc = get_config()

if __name__ == "__main__":
    parser = ArgumentParser()

    # Bool doesn't work as you'd expect...
    def str2bool(value):
        if isinstance(value, bool):
            return value
        if value.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        else:
            return False

    parser.add_argument("--debug", action="store_true", dest="debug",
                        help="Increase log level to debug")

    parser.add_argument("--demon", action="store_true", dest="demon",
                        help="Run in the background, do not generate stdout")

    parser.add_argument("--list", action="store_true", dest="listtags",
                        help="List Processing Tags")

    parser.add_argument("--domain", action="store", dest="domain", type=str,
                        default=None, help="Domain, for add, update, or list")

    parser.add_argument("--priority", action="store", dest="priority",
                        type=int, default=None,
                        help="Priority, for add, update or list")

    parser.add_argument("--published", action="store", dest="published",
                        type=str2bool, default=None,
                        help="published [bool], for add, update, or list")

    parser.add_argument("--description", action="store", dest="description",
                        help="Description of this processing tag")

    parser.add_argument("--addtag", action="store", dest="addtag", type=str,
                        default=None, help="Add new tag. Must specify domain"
                                           "and priority")

    parser.add_argument("--updatetag", action="store", dest="updatetag",
                        type=str, default=None,
                        help="Update this tag. Specify one or more of domain,"
                             "priorpty and/or published")

    options = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Announce startup
    logger.info("*** processing_tags.py - starting up at {}"
                .format(datetime.datetime.now()))
    logger.debug("Config files used: %s", ', '.join(fsc.configfiles_used))

    session = sessionfactory()

    if options.listtags:
        query = session.query(ProcessingTag)

        if options.domain is not None:
            query = query.filter(ProcessingTag.domain==options.domain)

        if options.priority is not None:
            query = query.filter(ProcessingTag.priority==options.priority)

        if options.published is not None:
            query = query.filter(ProcessingTag.published==options.published)

        query = query.order_by(ProcessingTag.domain).order_by(ProcessingTag.priority)

        ptags = query.all()
        for ptag in ptags:
            logger.info(str(ptag))

    if options.addtag:
        if options.domain is None:
            logger.error("Need to specify domain with addtag")
            exit(1)
        if options.priority is None:
            logger.error("Need to specify priortiy with addtag")
            exit(1)

        # Check if already exists
        query = session.query(ProcessingTag) \
            .filter(ProcessingTag.tag==options.addtag)

        try:
            query.one()
            logger.error(f"Processing tag with name {options.addtag} already"
                         f"exists. Aborting.")
            exit(4)
        except NoResultFound:
            pass
        
        tag = ProcessingTag(tag=options.addtag, domain=options.domain,
                            priority=options.priority,
                            published=options.published,
                            description=options.description)
        session.add(tag)
        session.commit()
        logger.info(f"Added tag: {str(tag)}")

    if options.updatetag:
        if options.domain is None and options.priority is None \
                and options.published is None and options.description is None:
            logger.error("Must specify at least one of domain, priority and/or"
                         "published and/or description to update tag")
            exit(1)
        query = session.query(ProcessingTag) \
            .filter(ProcessingTag.tag==options.updatetag)

        try:
            tag = query.one()
        except MultipleResultsFound:
            logger.error(f"Multiple tags found with tag {options.updatetag}!"
                         f"Aborting.")
            exit(2)
        except NoResultFound:
            logger.error(f"No tag found with tag {options.updatetag}. Aborting")
            exit(3)

        logger.info(f"Updating tag: {str(tag)}...")

        if options.domain is not None:
            logger.info(f"Setting domain to {options.domain}")
            tag.domain = options.domain

        if options.priority is not None:
            logger.info(f"Setting priority to {options.priority}")
            tag.priority = options.priority

        if options.published is not None:
            logger.info(f"Setting published to {options.published}")
            tag.published = options.published

        if options.description is not None:
            logger.info(f"Setting description to: {options.description}")
            tag.description = options.description

        session.commit()

    logger.info("*** processing_tags.py exiting normally at %s",
                datetime.datetime.now())
