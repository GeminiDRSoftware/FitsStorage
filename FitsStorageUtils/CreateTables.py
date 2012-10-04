"""
This module provides various utility functions for create_tables.py 
in the Fits Storage System.
"""
from FitsStorage import *
from FitsStorageConfig import using_apache
from FitsStorageConfig import using_sqlite


def create_tables(session):
  """
  Creates the database tables and grants the apache user
  SELECT on the appropriate ones
  """
  # Create the tables
  File.metadata.create_all(bind=pg_db)

  # Add the geometry types separately. this is postgres specific and referencing these column in local mode isn't going to work
  if (not using_sqlite):
    session.execute("ALTER TABLE footprint ADD COLUMN area polygon")
    session.commit()
    session.execute("ALTER TABLE photstandard ADD COLUMN coords point")
    session.commit()

  if (using_apache and not using_sqlite):
    # Now grant the apache user select on them for the www queries
    session.execute("GRANT SELECT ON file, diskfile, diskfilereport, header, fulltextheader, gmos, niri, michelle, gnirs, nifs, tape, tape_id_seq, tapewrite, taperead, tapefile, notification, photstandard, photstandardobs, footprint, qareport, qametriciq, qametriczp, qametricsb, qametricpe, authentication, gsafile TO apache");
    session.execute("GRANT INSERT,UPDATE ON tape, tape_id_seq, notification, notification_id_seq, qareport, qareport_id_seq, qametriciq, qametriciq_id_seq, qametriczp, qametriczp_id_seq, qametricsb, qametricsb_id_seq, qametricpe, qametricpe_id_seq, authentication, authentication_id_seq TO apache");
    session.execute("GRANT DELETE ON notification TO apache");
  session.commit()

def drop_tables(session):
  """
  Drops all the database tables. Very unsubtle. Use with caution
  """
  File.metadata.drop_all(bind=pg_db)
  session.commit()
