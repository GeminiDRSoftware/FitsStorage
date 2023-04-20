"""
This module provides various utility functions for ingest_standards.py
in the Fits Storage System.
"""
from fits_storage.core.orm.photstandard import PhotStandard
from fits_storage.core.geometryhacks import add_point
from fits_storage.logger import DummyLogger

mag_pairs = (
    (4, 'u_mag'),
    (5, 'v_mag'),
    (6, 'g_mag'),
    (7, 'r_mag'),
    (8, 'i_mag'),
    (9, 'z_mag'),
    (10, 'y_mag'),
    (11, 'j_mag'),
    (12, 'h_mag'),
    (13, 'k_mag'),
    (14, 'lprime_mag'),
    (15, 'm_mag')
)


def ingest_standards(session, filename, logger=DummyLogger()):
    """
    Load the standards text file into the Standards table
    """

    count = 0

    # Loop through entries in the standards text file, adding to table
    with open(filename, 'r') as standards:
        for line in standards:
            if line[0] == '#':
                continue

            fields = line.strip().split(',')

            # Do we already have an entry for this standard?
            std = session.query(PhotStandard).\
                filter(PhotStandard.name == fields[0]).first()
            if std is None:
                # Create an entry for it
                std = PhotStandard()
                session.add(std)
            else:
                logger.warning("Standard %s already exists. Updating from"
                               "provided file.", std.name)
            # Populate the details
            try:
                std.name = fields[0]
                std.field = fields[1]
                std.ra = 15.0*float(fields[2])
                std.dec = float(fields[3])
                for n, mag in mag_pairs:
                    if fields[n] != 'None':
                        setattr(std, mag, float(fields[n]))
            except ValueError:
                print("Fields: %s" % str(fields))
                raise

            session.commit()

            # Hack in the geometrical point column
            add_point(session, std.id, std.ra, std.dec)

            count += 1

    logger.info("Ingested %d standards" % count)