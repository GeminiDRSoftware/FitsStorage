"""
This module contains the gmoscal html generator function.
"""
import sqlalchemy
from sqlalchemy.sql.expression import cast
from sqlalchemy import join
from gemini_obs_db.header import Header
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.file import File

from ..utils.web import get_context, Return

from .selection import queryselection
from ..fits_storage_config import using_sqlite, fits_system_status, das_calproc_path
from gemini_obs_db.utils.gemini_metadata_utils import ONEDAY_OFFSET

from . import templating

import os
import datetime
from datetime import timedelta
import time
import json


@templating.templated("gmoscalbiasfiles.json")
def gmoscalbiasfiles(selection):
    """
    This generates a GMOS calbiration bias file list with logic similar to the `gmoscal` endpoint.
    """

    # TODO to be safe since this is patching into 2020-2, I copied code from
    # gmoscal.  These should be refactored once this is stable so we don't
    # have duplicated code.

    result = dict(
        file_list = list(),
        is_development = fits_system_status == 'development',
    )

    if using_sqlite:
        result['using_sqlite'] = True
        return Return.HTTP_NOT_IMPLEMENTED, result

    session = get_context().session

    # Was a date provided by user?
    datenotprovided = ('date' not in selection) and ('daterange' not in selection)
    # If no date or daterange, look on endor or josie to get the last processing date

    def autodetect_range(checkfile, selection):
        base_dir = das_calproc_path
        enddate = datetime.datetime.now().date()
        date = enddate
        found = -1000
        startdate = None

        ret = None
        while found < 0:
            datestr = date.strftime("%Y%b%d").lower()
            file = os.path.join(base_dir, datestr, checkfile)
            if os.path.exists(file):
                found = 1
                startdate = date
            date -= ONEDAY_OFFSET
            found += 1

            if startdate:
                # Start the day after the last reduction
                startdate += ONEDAY_OFFSET
                ret = "%s-%s" % (startdate.strftime("%Y%m%d"), enddate.strftime("%Y%m%d"))
                selection['daterange'] = ret

        return ret

    if datenotprovided:
        res = autodetect_range('Basecalib/biasall.list', selection)
        if res:
            result['bias_autodetected_range'] = res

    tzoffset = timedelta(seconds=(time.altzone if time.daylight else time.timezone))

    offset = sqlalchemy.sql.expression.literal(tzoffset - ONEDAY_OFFSET, sqlalchemy.types.Interval)
    query = (
        session.query(cast((Header.ut_datetime + offset), sqlalchemy.types.DATE).label('utdate'),
                      Header.detector_binning, Header.detector_roi_setting, DiskFile.filename)
            .select_from(join(join(DiskFile, File), Header))
            .filter(DiskFile.canonical == True)
        )

    # Fudge and add the selection criteria
    # Keep the same selection from the flats above, but drop the spectroscopy specifier and add some others
    if 'spectroscopy' in selection.keys():
        selection.pop('spectroscopy')
    selection['observation_type'] = 'BIAS'
    selection['inst'] = 'GMOS'
    selection['qa_state'] = 'NotFail'
    query = (
        queryselection(query, selection)
        )

    # OK, re-organise results into tally table dict
    # dict is: {utdate: {binning: {roi: Number}}
    bias = {}
    seendates = set()
    for utdate, binning, roi, filename in query:
        utdate = utdate.strftime('%Y-%m-%d')
        if utdate not in list(bias.keys()):
            bias[utdate] = {}
        if binning not in list(bias[utdate].keys()):
            bias[utdate][binning] = {}
        if roi not in list(bias[utdate][binning].keys()):
            bias[utdate][binning][roi] = list()
        bias[utdate][binning][roi].append(filename)
        seendates.add(utdate)

    result.update(dict(
        file_list=json.dumps(bias),
        ))

    return result
