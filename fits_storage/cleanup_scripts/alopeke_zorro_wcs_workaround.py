"""
Ricardo brought to my attention that RA/DEC are missing for many Alopeke/Zorro
data files.  This is different from the old RA--TAN bug in the data (though
that didn't help).

Ultimately, I decided to fix this with the custom AstroDataGemini subclass
in the FitsStorage code base that we register.  However, I need a cleanup
script to handle existing records without an overkill reingest.

a) query Zorro/Alopeke files for a given date/range
b) check if ra and dec are not set
c) pull CTYPE1/2 CRVAL1/2 information from the fulltextheader record
d) parse out ra/dec from that and update Header table

And run this on archive to avoid transfer costs
"""
from datetime import datetime

from sqlalchemy import or_

from fits_storage.gemini_metadata_utils import ratodeg, dectodeg
from fits_storage.orm import session_scope
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.fulltextheader import FullTextHeader
from fits_storage.orm.header import Header


def parse_value(line):
    val = line
    if val.index('=') >= 0:
        idx = val.index('=') + 1
        val = val[idx:]
    try:
        if val.index("\'") >= 0:
            idx = val.index("\'") + 1
            val = val[idx:]
        if val.index("\'") >= 0:
            idx = val.index("\'")
            val = val[:idx]
    except ValueError:
        pass
    if val is not None:
        val = val.strip()
    return val


if __name__ == "__main__":
    instrument = "ZORRO"  # "ALOPEKE"
    from_dt = datetime(2010,1,1)
    to_dt = datetime(2021,12,30)

    if instrument.upper() not in ('ALOPEKE', 'ZORRO'):
        print("Specify instrument (alopeke or zorro)")
        exit(1)

    print("Running instrument of: %s" % instrument)
    print("Running date range of: %s to %s" % (from_dt, to_dt))

    with session_scope() as session:
        for hdr, fht, df in session.query(Header, FullTextHeader, DiskFile).filter(Header.instrument == instrument) \
                .filter(Header.ut_datetime.between(from_dt, to_dt)) \
                .filter(or_(Header.ra == None, Header.dec == None)) \
                .filter(FullTextHeader.diskfile_id ==DiskFile.id) \
                .filter(Header.diskfile_id == DiskFile.id) \
                .filter(DiskFile.canonical == True) \
                .all():
            ctype1 = None
            ctype2 = None
            crval1 = None
            crval2 = None
            ra = None
            dec = None
            header_text = fht.fulltext
            if header_text is not None:
                lines = header_text.split('\n')
                for line in lines:
                    if line.startswith('CTYPE1'):
                        ctype1 = parse_value(line)
                    if line.startswith('CTYPE2'):
                        ctype2 = parse_value(line)
                    if line.startswith('CRVAL1'):
                        crval1 = parse_value(line)
                    if line.startswith('CRVAL2'):
                        crval2 = parse_value(line)
            if ctype1 == 'RA---TAN' or ctype1 == 'RA--TAN':
                ra = crval1
            if ctype2 == 'RA---TAN' or ctype2 == 'RA--TAN':
                ra = crval2
            if ctype1 == 'DEC--TAN':
                dec = crval1
            if ctype2 == 'DEC--TAN':
                dec = crval2
            if type(ra) is str:
                ra = ratodeg(ra)
            if type(dec) is str:
                dec = dectodeg(dec)
            if ra is not None and dec is not None:
                print("Would update %s to ra, dec = %s, %s" % (df.filename, ra, dec))
            hdr.ra = ra
            hdr.dec = dec
        session.commit()
