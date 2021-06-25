#! /usr/bin/env python
import sys
import signal
import datetime
import traceback
from sqlalchemy import join, or_, desc

from gemini_obs_db import session_scope
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.header import Header
from gemini_obs_db.gmos import Gmos
from gemini_obs_db.niri import Niri
from gemini_obs_db.gnirs import Gnirs
from gemini_obs_db.nifs import Nifs
from gemini_obs_db.michelle import Michelle
from gemini_obs_db.f2 import F2
from fits_storage.logger import logger, setdebug, setdemon
from optparse import OptionParser

# Define signal handler. This allows us to bail out neatly if we get a signal
def handler(signum, frame):
    logger.info("Received signal: %d " % signum)
    raise Exception('Signal', signum)


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("--inst", action="store", dest="inst", help="Instrument table to rebuild")
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

    options, args = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)


    # Set handlers for the signals we want to handle
    signal.signal(signal.SIGHUP, handler)
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGQUIT, handler)
    signal.signal(signal.SIGTERM, handler)

    # Annouce startup
    now = datetime.datetime.now()
    logger.info("*********    rebuild_inst_table.py - starting up at %s" % now)

    if not options.inst:
        print("Specify the instrument table to rebuild")
        sys.exit(1)

    inst = options.inst
    already = None

    inst_data = {
        'gmos':     ('GMOS', Gmos, or_(Header.instrument == 'GMOS-N', Header.instrument == 'GMOS-S')),
        'niri':     ('NIRI', Niri, Header.instrument == 'NIRI'),
        'gnirs':    ('GNIRS', Gnirs, Header.instrument == 'GNIRS'),
        'nifs':     ('NIFS', Nifs, Header.instrument == 'NIFS'),
        'michelle': ('MICHELLE', Michelle, Header.instrument == 'michelle'),
        'f2':       ('F2', F2, Header.instrument == 'f2')
        }

    instrName, instrClass, filt = inst_data[inst]

    with session_scope() as session:
        try:
            # Get a list of header ids for which there is a present diskfile for this instrument
            logger.info("Rebuilding {} table".format(instrName))
            headers = (
                session.query(Header).select_from(join(Header, DiskFile))
                            .filter(filt)
                            .filter(DiskFile.present == True)
                            .order_by(desc(Header.ut_datetime))
                )
            logger.info("Found %s files to process" % headers.count())

            for i, header in enumerate(headers, 1):
                # Does an instheader for this header id already esist?
                already = session.query(instrClass).filter(instrClass.header_id == header.id).count()

                if not already:
                    # No, we should add it.
                    logger.info("Processing %d/%d" % (i, count))
                    session.add(instrClass(header))
                    session.commit()

        except:
            logger.error("Exception: %s : %s" % (sys.exc_info()[0], sys.exc_info()[1]))
            traceback.print_tb(sys.exc_info()[2])

    logger.info("*********    rebuild_inst_table.py - exiting at %s" % datetime.datetime.now())
