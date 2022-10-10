"""
Run New Photometric Standards

This is a helper script to map new photometric standards against existing headers.
It is still a somewhat manual process that can be run against headers in batches
by date range, or for a specific header by ID.  You need to pass the list of ids
for the new photometric standards.  This does not delete existing links and should
therefore not be used against existing photometric standards.
"""
from sqlalchemy import or_

# pick up DB settings for FITS Storage
import fits_storage

from gemini_obs_db.db import session_scope
from gemini_obs_db.orm.header import Header
from gemini_obs_db.orm.diskfile import DiskFile
from datetime import datetime


def do_std_obs_for_new(session, header_id, photstds):
    """
    This custom version  only looks vs the new footprints
    """
    sql = "insert into photstandardobs (select nextval('photstandardobs_id_seq') as id, " \
          "photstandard.id AS photstandard_id, footprint.id AS footprint_id from " \
          "photstandard, footprint where photstandard.id in (%s) and " \
          "photstandard.coords @ footprint.area and footprint.header_id=%d)" % (photstds, header_id)
    result = session.execute(sql)
    session.commit()

    if result.rowcount:
        header = session.query(Header).get(header_id)
        header.phot_standard = True
        session.commit()


if __name__ == "__main__":
    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--headerid", action="store", type="string", dest="headerid", help="Run vs specific header id")
    parser.add_option("--fromdt", action="store", type="string", dest="fromdt", help="Run headers from this date (inclusive)")
    parser.add_option("--todt", action="store", type="string", dest="todt", help="Run headers to this date (exclusive)")
    parser.add_option("--photstds", action="store", type="string", dest="photstds", help="Comma separated list of phot standard ids to run")
    parser.add_option("--instrument", action="store", type="string", dest="instrument",
                      help="Instrument to run against, optional, i.e. GMOS-N.  "
                           "Use GMOS to run vs both GMOS-N and GMOS-S")

    (options, args) = parser.parse_args()

    if not options.photstds:
        print("List of photometric standard ids (not names) required")
        exit(1)

    if options.fromdt and options.todt:
        fromdt = datetime.strptime(options.fromdt, "%Y%m%d")
        todt = datetime.strptime(options.todt, "%Y%m%d")
        print(f"Running vs date range: {fromdt} - {todt}")
    if options.headerid:
        print(f"Running vs header {options.headerid}")

    with session_scope() as session:
        if options.fromdt and options.todt:
            q = session.query(Header, DiskFile).filter(Header.ut_datetime >= fromdt) \
                    .filter(Header.ut_datetime < todt) \
                    .filter(Header.diskfile_id == DiskFile.id) \
                    .filter(DiskFile.canonical == True)
            if options.instrument:
                if options.instrument == "GMOS":
                    q = q.filter(or_(Header.instrument == "GMOS-N", Header.instrument == "GMOS-S"))
                else:
                    q = q.filter(Header.instrument == options.instrument)
            for h, _ in q:
                do_std_obs_for_new(session, h.id, options.photstds)
        if options.headerid:
                do_std_obs_for_new(session, options.headerid, options.photstds)

