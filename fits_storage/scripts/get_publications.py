#! /usr/bin/env python3

from optparse import OptionParser
from datetime import datetime
from sqlalchemy.exc import NoResultFound, MultipleResultsFound

from fits_storage.server.publications_db_interface import get_publications
from fits_storage.server.orm.publication import Publication, ProgramPublication
from fits_storage.server.orm.program import Program

from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.db import session_scope
from fits_storage.config import get_config


fsc = get_config()

parser = OptionParser()
parser.add_option("--jsonfile", action="store", dest="jsonfile",
                  help="Read this json file rather than querying the web "
                       "server")
parser.add_option("--debug", action="store_true", dest="debug", default=False,
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False,
                  help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

logger.info("*** get_publications.py starting up at %s", datetime.now())

if options.jsonfile:
    with open(options.jsonfile, "r") as fp:
        json_inject = fp.read()
else:
    json_inject = None

publications = get_publications(json_inject=json_inject, logger=logger)

logger.info("Got %d publications from web server", len(publications))

n = 0  # Number of publications processed
m = 0  # Number of warnings or errors
with session_scope() as session:
    for pub in publications:
        n += 1
        bibcode = pub.get('bibcode')
        if bibcode is None:
            logger.warning("Got a publication from the web server with no"
                           "bibcode - id %s", pub.get('id'))
            m += 1
            continue
        logger.debug("Processing publication %s", bibcode)

        # Do we have a publication with this bibcode in the database?
        try:
            pub_orm = session.query(Publication)\
                .filter(Publication.bibcode == bibcode).one()
        except MultipleResultsFound:
            logger.error("Multiple publications in database with bibcode %s"
                         % bibcode)
            m += 1
            continue
        except NoResultFound:
            logger.debug("Creating new publication database entry for %s",
                         bibcode)
            pub_orm = Publication(bibcode)

        # Update the values. The JSON dict keys are the same as the names of
        # the date items in the ORM object.
        for thing in ['author', 'title', 'year', 'journal', 'telescope',
                      'instrument', 'country', 'wavelength', 'mode', 'gstaff',
                      'gsa', 'golden', 'too', 'partner']:
            setattr(pub_orm, thing, pub[thing])

        for pid in pub['program_ids']:
            # Find this program in the programs table
            try:
                prog = session.query(Program)\
                    .filter(Program.program_id == pid).one()
            except NoResultFound:
                logger.warning("Publication %s references Program %s "
                               "which is not in programs table", bibcode, pid)
                m += 1
                continue

            # At this point, pub_orm is the publication ORM object, and
            # prog is the program ORM object.
            if pub_orm not in prog.publications:
                prog.publications.add(pub_orm)

        # OK, we're done with this publication
        session.commit()

logger.info("Processed %d publications, with %d warnings or errors", n, m)
logger.info("*** get_publications.py exiting at %s", datetime.now())
