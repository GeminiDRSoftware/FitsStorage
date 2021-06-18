

"""
Ricardo brought to my attention that the RA/DEC were not parsing out.  This
is because they put the RA and DEC in the phu.  I have a fix for ingesting
'Alopeke/Zorro going forward, but I need a script to clean up the old entries

a) query Zorro/Alopeke files for a given date/range where ra=NULL
b) get the header text and parse out the ra and dec values from there
c) update the ra and dec fields in the Header record
"""
import os
import re
from datetime import datetime, timedelta

from fits_storage.orm import session_scope
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.header import Header
from fits_storage.orm.fulltextheader import FullTextHeader
#from fits_storage.scripts.header_fixer2 import open_image
from fits_storage.utils.hashes import md5sum


def get_records(session, instrument, prefix):
        # Get a list of all diskfile_ids marked as present
        q = session.query(DiskFile, Header, FullTextHeader) \
            .filter(Header.diskfile_id == DiskFile.id) \
            .filter(DiskFile.canonical) \
            .filter(Header.instrument == instrument.upper()) \
            .filter(DiskFile.filename.like("%s%%" % prefix)) \
            .filter(Header.ra == None) \
            .filter(FullTextHeader.diskfile_id == DiskFile.id)
        for df, h, ht in q:
            yield df, h, ht


def _parse_string_header(line):
    m = re.search(r'=\s*\'?([^/\']+)\'?\s*/', line)
    if m:
        retval = m.groups(1)[0].strip()
        return retval
    return None


def _parse_float_header(line):
    m = re.search(r'=\s*\'?([-\+\.\d]+)\'?\s*/', line)
    if m:
        numtxt = m.groups(1)[0]
        try:
            return float(numtxt)
        except:
            return None
    return None


if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--instrument", action="store", type="string", default="zorro", dest="instrument", help="instrument to correct (zorro or alopeke).")
    parser.add_option("--prefix", action="store", type="string", default="none", dest="prefix", help="filename prefix ([SN]YYYYMMDD).")

    (options, args) = parser.parse_args()

    instrument = options.instrument
    if instrument.upper() not in ('ALOPEKE', 'ZORRO'):
        print("Specify instrument (alopeke or zorro)")
        exit(1)

    prefix = options.prefix

    print("Running instrument of: %s" % instrument)
    print("Saw prefix to run of: %s" % prefix)

    with session_scope() as session:
        try:
            for df, h, ht in get_records(session, instrument, prefix):
                filename = df.filename
                if h and h.ra is None and h.dec is None and ht is not None and ht.fulltext is not None:
                    phulines = ht.fulltext.split('\n')
                    ctype1 = None
                    ctype2 = None
                    crval1 = None
                    crval2 = None
                    ra = None
                    dec = None
                    for line in phulines:
                        if line.startswith('CTYPE1 '):
                            val = _parse_string_header(line)
                            if val is not None:
                                ctype1 = val
                        if line.startswith('CTYPE2 '):
                            val = _parse_string_header(line)
                            if val is not None:
                                ctype2 = val
                        if line.startswith('CRVAL1 '):
                            val = _parse_float_header(line)
                            if val is not None:
                                crval1 = val
                        if line.startswith('CRVAL2 '):
                            val = _parse_float_header(line)
                            if val is not None:
                                crval2 = val
                    if ctype1 and ctype2 and crval1 and crval2:
                        if ctype1 == 'RA---TAN' or ctype1 == 'RA--TAN':
                            ra = crval1
                        if ctype2 == 'RA---TAN' or ctype2 == 'RA--TAN':
                            ra = crval2
                        if ctype1 == 'DEC--TAN':
                            dec = crval1
                        if ctype2 == 'DEC--TAN':
                            dec = crval2
                    if ra is not None and dec is not None:
                        print("Updating %s to ra=%s dec=%s" % (filename, ra, dec))
                        h.ra = ra
                        h.dec = dec
                        session.flush()
                    else:
                        print("Unable to repair %s" % filename)
        finally:
            session.commit()

