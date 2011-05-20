"""
This module provides various utility functions to work around the lack
of support for simple geometry types in sqlalchemy.
This could be replaced with a more accuate sysyem using postGIS to do the 
co-ordinate transforms properly in the future
"""
from FitsStorage import *


def add_footprint(session, id, fp):
  """
  Sets the area column of the footprint table to be a polygon defined in fp
  """
  fptext = "'((%f, %f), (%f, %f), (%f, %f), (%f, %f))'" % (fp[0][0], fp[0][1], fp[1][0], fp[1][1], fp[2][0], fp[2][1], fp[3][0], fp[3][1])

  session.execute("UPDATE footprint set area = %s where id=%d" % (fptext, id))
  session.commit()

def add_point(session, id, x, y):
  """
  Sets the coords column of the photstandard table to be a point defined by (x, y)
  """
  ptext = "'(%f, %f)'" % (x, y)
  session.execute("UPDATE photstandard set coords = %s WHERE id=%d" % (ptext, id))
  session.commit()

def do_std_obs(session, header_id):
  """
  Populates the PhotStandardObs table wrt reference to the given header id
  Also sets the flag in the Header table to say it has a standard
  """
  sql = "insert into photstandardobs (select nextval('photstandardobs_id_seq') as id, photstandard.id AS photstandard_id, footprint.id AS footprint_id from photstandard, footprint where photstandard.coords @ footprint.area and footprint.header_id=%d)" % header_id
  result = session.execute(sql)
  session.commit()

  if(result.rowcount):
    header = session.query(Header).filter(Header.id == header_id).one()
    header.phot_standard = True
    session.commit()
