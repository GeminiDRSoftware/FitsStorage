"""
This module provides various utility functions for ingest_standards.py
in the Fits Storage System.
"""
from ..orm.photstandard import PhotStandard
from ..orm.geometryhacks import add_point

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

def ingest_standards(session, filename):
    """
    Load the standards text file into the Standards table
    """

    # Loop through entries in the standards text file, adding to table
    for line in open(filename, 'r'):
        if line[0] != '#':
            fields = line.strip().split(',')

            # Create and populate a standard instance
            std = PhotStandard()
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

            # Add to database session
            session.add(std)
            session.commit()

            # Hack in the geometrical point column
            add_point(session, std.id, std.ra, std.dec)
