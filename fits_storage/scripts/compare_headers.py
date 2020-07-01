#/usr/bin/env python

import os
import bz2
import sys
import pprint

from astropy.io.fits import open as pfopen
from astropy.io.fits.verify import VerifyError

from fits_storage.logger import logger, setdebug, setdemon

from fits_storage import orm
from fits_storage.orm.resolve_versions import Version
from sqlalchemy import distinct

def compare_headers(*headers):
    differences = []

    for n, hdu in enumerate(zip(*headers), 0):
        if None in hdu:
            differences.append('HDU {0}: Missing in some file'.format(n))
        hdu_diff = []
        differences.append(('HDU {0}'.format(n), hdu_diff))
        cards = set()
        for h in hdu:
            cards.update(h)
        for c in cards:
            values = set()
            for h in hdu:
                values.add(h.get(c))
            if len(values) > 1:
                hdu_diff.append((c, list(values)))

    return differences

def extract_header(path):
    f = pfopen(bz2.BZ2File(path))
    f.verify('fix')
    return [hdu.header for hdu in f]


if __name__ == "__main__":

    setdebug(False)
    setdemon(False)
    logger.info("*********    compare_headers.py - starting up")

    with orm.session_scope() as session:
        unable = (session.query(distinct(Version.filename))
                         .filter(Version.unable == True))
        for (fname,) in unable:
            logger.info('Comparing instances of {0}'.format(fname))
            prefix = fname[:6]
            d = os.path.join('/data/differences', prefix)
            try:
                os.makedirs(d)
            except os.error:
                if not os.path.exists(d):
                    logger.error('Could not create {0}'.format(d))
                    continue

            with open(os.path.join(d, fname + '.log'), 'w') as logfile:
                try:
                    sys.stderr = logfile

                    files = session.query(Version).filter(Version.filename == fname)
                    headers = []
                    for f in files:
                        print(f.fullpath, file=logfile)
                        headers.append(extract_header(f.fullpath))
                        f.unable = False
                        f.score = -1

                    pprint.pprint(compare_headers(*headers), stream = logfile)
                    session.commit()
                except (VerifyError, IOError) as e:
                    print(e, file=logfile)
                finally:
                    sys.stderr = sys.__stderr__
