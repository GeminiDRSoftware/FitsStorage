"""This module provides various utility functions to work around the lack of
support for simple geometry types in sqlalchemy. This could be replaced with
a more accurate system using postGIS to do the co-ordinate transforms properly
in the future.

"""
import sys
import traceback
from sqlalchemy import text
from .orm.header import Header


def add_footprint(session, id, fp):
    """
    Sets the area column of the footprint table to be a polygon defined in fp.

    Parameters
    ----------
    session : :class:`sqlalchemy.orm.session.Session`
        SQL Alchemy session to use to add footprint
    id : int
        ID of the :class:`~footprint.Footprint` record to update
    fp : float[4,2]
        array of footprint coordinates
    """
    form1 = "'(({}, {}), ({}, {}), ({}, {}), ({}, {}))'" 

    fptext = form1.format(fp[0][0], fp[0][1], fp[1][0], fp[1][1], fp[2][0],
                          fp[2][1], fp[3][0], fp[3][1])
    session.execute(text("UPDATE footprint set area = {} where id={}".format(fptext, id)))

def add_point(session, id, x, y):
    """
    Sets the coords column of the photstandard table to be a point defined by
    (x, y).

    Parameters
    ----------
    session : :class:`sqlalchemy.session.Session`
        SLQ Alchemy session to use to add the photstandard
    id : int
        ID of the photstandard
    x : float
        X coordinate
    y : float
        Y coordinate
    """
    ptext = "'({}, {})'".format(x, y)
    session.execute(text("UPDATE photstandard set coords = {} WHERE id={}".
                    format(ptext, id)))
    session.commit()


def do_std_obs(session, header_id, commit=True):
    """
    Populates the PhotStandardObs table wrt reference to the given header id
    Also sets the flag in the Header table to say it has a standard.

    Parameters
    ----------
    session : :class:`sqlalchemy.session.Session`
        SQL Alchemy session to use to update photstandardobs
    header_id : int
        ID of corresponding header for the footprint
    """
    try:
        sql = "insert into photstandardobs (select nextval('photstandardobs_id_seq') as id, photstandard.id AS photstandard_id, footprint.id AS footprint_id from photstandard, footprint where photstandard.coords <@ footprint.area and footprint.header_id=%d)" % header_id
        result = session.execute(text(sql))
        if result.rowcount:
            header = session.get(Header, header_id)
            header.phot_standard = True

        if commit:
            session.commit()
    except Exception:
        traceback.print_exc(file=sys.stdout)
        session.rollback()
