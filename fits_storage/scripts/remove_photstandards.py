"""
Remove Photometric Standards

This is a helper script to remove deprecated photometric standards and all mappings
to existing headers.

It is a somewhat manual process that can be run against headers in batches
by date range, or for a specific header by ID.  You need to pass the list of names
for the new photometric standards.
"""
from sqlalchemy import or_

# pick up DB settings for FITS Storage
import fits_storage

from gemini_obs_db.db import session_scope
from gemini_obs_db.orm.header import Header
from gemini_obs_db.orm.diskfile import DiskFile
from fits_storage.orm.photstandard import PhotStandard
from datetime import datetime


def delete_std_obs(session, photstds):
    """
    This custom version  only looks vs the new footprints
    """
    ids = set()
    for name in photstds.split(","):
        q = session.query(PhotStandard).filter(PhotStandard.name == name)
        ps = q.first()
        if ps is None:
            raise Exception("Phot. Standard not found for %s" % name)
        ids.add(ps.id)
    ids = list(ids)
    ids.sort()
    ids = ",".join([str(id) for id in ids])

    sql = "delete from photstandardobs where photstandard_id in (%s)" % ids
    result = session.execute(sql)
    session.flush()
    sql = "delete from photstandard where id in (%s)" % ids
    session.commit()

    # now clear any headers with standards flags that no longer have a standard in field
    for header in session.query(Header).filter(Header.phot_standard==True).all():
        photstds = session.query(PhotStandardObs, Footprint).filter(PhotStandardObs.footprint_id==Footprint.id).filter(Footprint.header_id==header.id)
        if photstds.count() == 0:
            header.phot_standard = False
            session.add(header)
    session.commit()


if __name__ == "__main__":
    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--photstds", action="store", type="string", dest="photstds", help="Comma separated list of phot standard ids to run")

    (options, args) = parser.parse_args()

    if not options.photstds:
        print("List of photometric standard names required")
        exit(1)

    with session_scope() as session:
        delete_std_obs(session, options.photstds)
