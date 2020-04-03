"""
This module provides various utility functions to manage and service the target
queue.

"""
import os, sys, traceback
import datetime
from sqlalchemy import desc
from sqlalchemy.orm.exc import ObjectDeletedError
from sqlalchemy.orm import make_transient

import numpy
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from ..orm.header import Header
from ..orm.diskfile import DiskFile
from ..orm.target import Target, TargetsChecked, TargetPresence
from ..orm.target import TargetQueue

from . import queue
import functools

from ..fits_storage_config import using_s3
from ..fits_storage_config import storage_root
from ..fits_storage_config import preview_path
from ..fits_storage_config import z_staging_area

import bz2

if using_s3:
    from ..fits_storage_config import s3_staging_area
    from .aws_s3 import get_helper

import astrodata
import gemini_instruments
from gemini_instruments.gmos.pixel_functions import get_bias_level
from gempy.library.spectral import Spek1D

from astropy.time import Time, TimeDatetime
from astropy.coordinates import solar_system_ephemeris, EarthLocation
from astropy.coordinates import get_body_barycentric, get_body, get_moon

from .. import logger

# ------------------------------------------------------------------------------
def norm(data, percentile=0.3):
    """
    Normalize the data onto 0:1 using percentiles
    """
    lower = percentile
    upper = 100.0 - percentile
    plow = numpy.percentile(data, lower)
    phigh = numpy.percentile(data, upper)
    data = numpy.clip(data, plow, phigh)
    data -= plow
    data /= (phigh - plow)
    return data


def get_location(diskfile):
    # TODO n/s locations?
    loc = EarthLocation.of_site('greenwich')


def get_time(session, header):
    if header is not None and header.ut_datetime is not None:
        return Time(header.ut_datetime, scale='utc')
    return None


def check_contained(session, coords, header_id):
    # logic based on orm.geomhacks
    sql = "select id from footprint where POINT(%f,%f) @ footprint.area and footprint.header_id=%d" % (coords.ra.degree, coords.dec.degree, header_id)    print("SQL:\n%s" % sql)
    result = session.execute(sql)

    if result.rowcount:
        return True


class TargetQueueUtil(object):
    def __init__(self, session, logger):
        self.s = session
        self.l = logger
        self.targets = self.s.query(Target).all()

    def length(self):
        return queue.queue_length(TargetQueue, self.s)

    def pop(self):
        return queue.pop_queue(TargetQueue, self.s, self.l)

    def set_error(self, trans, exc_type, exc_value, tb):
        "Sets an error message to a transient object"
        #queue.add_error(TargetQueue, trans, exc_type, exc_value, tb, self.s)

    def delete(self, trans):
        "Deletes a transient object"
        queue.delete_with_id(TargetQueue, trans.id, self.s)

    def process(self, diskfiles, make=False):
        try:
            iter(diskfiles)
        except TypeError:
            # Didn't get an iterable; Assume we were passed a single diskfile or
            # previewqueue
            diskfiles = (diskfiles,)

        if make:
            # Go ahead and make the target now
            for df in diskfiles:
                if isinstance(df, TargetQueue):
                    tq = df
                    df = self.s.query(DiskFile).get(tq.diskfile_id)
                    message = "Making Target List for {}: {}".format(tq.id, df.filename)
                else:
                    message = "Making Target List with diskfile_id {}".format(df.id)
                self.l.info(message)
                if df.present == True:
                    self.make_targets(df)
                else:
                    self.l.info("Skipping non-present diskfile_id {}".format(df.id))
        else:
            # Add it to the target queue
            for df in diskfiles:
                if isinstance(df, TargetQueue):
                    tq = df
                    df = self.s.query(DiskFile).get(tq.diskfile_id)
                self.l.info("Adding TargetQueue with diskfile_id {}".format(df.id))
                tq = TargetQueue(df)
                self.s.add(tq)
            self.s.commit()

    def make_targets(self, diskfile):
        """
        Make the target list, given the diskfile.
        This can be called from within service_target_queue ingest_file.
        """
        # save the time for when we did this check
        now = datetime.datetime.now()

        header = self.s.query(Header).filter(Header.diskfile_id == diskfile.id).one_or_none()
        if header is not None:
            # If this file was already checked, we can just exit
            q = self.s.query(TargetsChecked).filter(TargetsChecked.diskfile_id == diskfile.id)
            if q.one_or_none() is not None:
                return

            # Add entries to target table
            loc = get_location(diskfile)
            t = get_time(self.s, header)
            if t is not None:
                with solar_system_ephemeris.set('de432s'):
                    for target in self.targets:
                        coords = get_body(target.ephemeris_name, t, loc)
                        # TODO how to check overlap?
                        if check_contained(self.s, coords, header.id):
                            self.s.add(TargetPresence(diskfile.id, target.name))
        self.s.add(TargetsChecked(diskfile.id, now))
