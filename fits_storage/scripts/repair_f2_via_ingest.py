

"""
It turns out, F2 data was mistakenly setting gcal_lamp to 'Off'
if GCALSHUT was CLOSED even for QH type GCALLAMP.  This was
being done by a custom gcal_lamp descriptor for F2 which is no
longer necessary.  We've removed the descriptor in DRAGONS, but
the existing F2 data needs to be reingested with force, since
the files themselves have not changed.

CalCache will also need rebuilding
"""
import os

from gemini_obs_db.db import session_scope
from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.header import Header


def get_files(session, startdt, enddt):
        # Get a list of all diskfile_ids marked as present
        q = session.query(DiskFile, Header) \
            .filter(Header.diskfile_id == DiskFile.id) \
            .filter(DiskFile.canonical) \
            .filter(Header.instrument == "F2") \
            .filter(_or(Header.observation_type == "flat", Header.observation_type == "arc")) \
            .filter(Header.ut_datetime >= startdt) \
            .filter(Header.ut_datetime < enddt)
        for df, h in q:
            yield df


if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--startdt", action="store", type="string", default="none", dest="staging", help="Starting date range YYYYMMDD (inclusive)")
    parser.add_option("--enddt", action="store", type="string", default="none", dest="staging", help="Ending date range YYYYMMDD (exclusive)")

    (options, args) = parser.parse_args()

    startdt = options.startdt
    enddt = options.enddt

    with session_scope() as session:
        try:
            iq = IngestQueueUtil(session, logger)
            for df in get_files(session, instrument, prefix):
                filename = df.filename
                print(filename)
                #iq.add_to_queue(filename, path=None, force=True)
        finally:
            session.commit()
