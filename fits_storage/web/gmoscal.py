"""
This module contains the gmoscal html generator function.
"""
import sqlalchemy
from sqlalchemy.sql.expression import cast
from sqlalchemy import func, join, desc
from gemini_obs_db.gmos import Gmos
from gemini_obs_db.header import Header
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.file import File

from ..utils.web import get_context, Return, with_content_type

from .selection import sayselection, queryselection
from .calibrations import interval_hours
from ..cal import get_cal_object
from ..fits_storage_config import using_sqlite, fits_system_status, das_calproc_path
from ..gemini_metadata_utils import gemini_time_period_from_range, ONEDAY_OFFSET

from . import templating

from math import fabs

import os
import copy
import datetime
from datetime import timedelta
import time
import re
import dateutil.parser
import json
from collections import defaultdict, namedtuple

@templating.templated("gmoscal.html")
def gmoscal_html(selection):
    return gmoscal(selection)


def gmoscal_json(selection):
    result = {
        'selection': selection
        }

    values = gmoscal(selection)

    if 'flat_autodetected_range' in values:
        result['Twilight_AutoDetectedDates'] = values['flat_autodetected_range']
    if 'bias_autodetected_range' in values:
        result['Bias_AutoDetectedDates'] = values['bias_autodetected_range']

    jlist = [{'n_sci': nsci, 'n_twilight': ntwi, 'filter': filt, 'binning': binn}
             for key, (nsci, ntwi, filt, binn) in values['twilight']]
    result['twilight_flats'] = jlist
    result['biases'] = dict((k.strftime("%Y%m%d"), v) for k, v in values['bias'])

    get_context().resp.send_json([result], indent=4)

def gmoscal(selection):
    """
    This generates a GMOS imaging twilight flat, bias and nod and shuffle darks report.
    If no date or daterange is given, tries to find last processing date
    """

    result = dict(
        said_selection = sayselection(selection),
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
        res = autodetect_range('Basecalib/flatall.list', selection)
        if res:
            result['flat_autodetected_range'] = res

    # We do this twice, first for the science data, then for the twilight flat data
    # These are differentiated by being science or dayCal

    twilight = {}
    # Put the results into dictionaries, which we can then combine into one html table or json items
    for observation_class in ('science', 'dayCal'):
        # The basic query for this
        query = (
            session.query(func.count(1), Header.filter_name, Header.detector_binning)
                .select_from(join(join(DiskFile, File), Header))
                .filter(DiskFile.canonical == True)
            )

        # Fudge and add the selection criteria
        selection['observation_class'] = observation_class
        selection['observation_type'] = 'OBJECT'
        selection['spectroscopy'] = False
        selection['inst'] = 'GMOS'
        selection['qa_state'] = 'NotFail'

        if observation_class == 'dayCal':
            selection['qa_state'] = 'Lucky'
            # Only select full frame dayCals
            query = query.filter(Header.detector_roi_setting == 'Full Frame')
            # Twilight flats must have the target name 'Twilight'
            query = query.filter(Header.object == 'Twilight')

        query = queryselection(query, selection)

        # Knock out ENG programs
        query = query.filter(Header.engineering == False).filter(Header.science_verification == False)

        # Group by clause
        query = query.group_by(Header.filter_name, Header.detector_binning).order_by(Header.detector_binning, Header.filter_name)

        # Populate the dictionary
        # as {'i-2x2':[10, 'i', '2x2'], ...}    ie [number, filter_name, binning]

        for cnt, filt, binn in query:
            # row[0] = count, [1] = filter, [2] = binning
            key = "%s-%s" % (filt, binn)
            if observation_class == 'science':
                twilight[key] = [cnt, 0, filt, binn]
            else:
                try:
                    twilight[key][1] = cnt
                except KeyError:
                    twilight[key] = [0, cnt, filt, binn]

    datething = ''
    if 'date' in selection:
        datething = selection['date']
    if 'daterange' in selection:
        datething = selection['daterange']

    if datenotprovided:
        res = autodetect_range('Basecalib/biasall.list', selection)
        if res:
            result['bias_autodetected_range'] = res

    tzoffset = timedelta(seconds=(time.altzone if time.daylight else time.timezone))

    offset = sqlalchemy.sql.expression.literal(tzoffset - ONEDAY_OFFSET, sqlalchemy.types.Interval)
    query = (
        session.query(func.count(1), cast((Header.ut_datetime + offset), sqlalchemy.types.DATE).label('utdate'), Header.detector_binning, Header.detector_roi_setting)
            .select_from(join(join(DiskFile, File), Header))
            .filter(DiskFile.canonical == True)
        )

    # Fudge and add the selection criteria
    # Keep the same selection from the flats above, but drop the spectroscopy specifier and add some others
    selection.pop('spectroscopy')
    selection['observation_type'] = 'BIAS'
    selection['inst'] = 'GMOS'
    selection['qa_state'] = 'NotFail'
    query = (
        queryselection(query, selection)
            .group_by('utdate', Header.detector_binning, Header.detector_roi_setting)
            .order_by('utdate', Header.detector_binning, Header.detector_roi_setting)
        )

    # OK, re-organise results into tally table dict
    # dict is: {utdate: {binning: {roi: Number}}
    bias = {}
    total_bias = defaultdict(int)
    for num, utdate, binning, roi in query:
        if utdate not in list(bias.keys()):
            bias[utdate] = {}
        if binning not in list(bias[utdate].keys()):
            bias[utdate][binning] = {}
        if roi not in list(bias[utdate][binning].keys()):
            bias[utdate][binning][roi] = num

        total_bias['%s-%s' % (binning, roi)] += num

    # OK, find if there were dates for which there were no biases...
    # Can only do this if we got a daterange selection, otherwise it's broken if there's none on the first or last day
    # utdates is a reverse sorted list for which there were biases.
    nobiases = []
    if 'daterange' in selection:
        # Parse the date to start and end datetime objects
        date, enddate = gemini_time_period_from_range(selection['daterange'], as_date=True)

        while date <= enddate:
            if date not in bias:
                nobiases.append(str(date))
            date += ONEDAY_OFFSET

        if nobiases:
            result['nobiases'] = nobiases

    # Nod & Shuffle Darks
    # The basic query for this
    def nod_and_shuffle(selection):
        session = get_context().session
        query = session.query(Header).select_from(join(join(Header, DiskFile), Gmos))

        # Fudge and add the selection criteria
        selection = {}
        selection['canonical'] = True
        selection['observation_class'] = 'science'
        selection['observation_type'] = 'OBJECT'
        selection['inst'] = 'GMOS'
        selection['qa_state'] = 'Pass'

        query = queryselection(query, selection)

        # Only Nod and Shuffle frames
        query = query.filter(Gmos.nodandshuffle == True)

        # Knock out ENG programs and SV programs
        query = query.filter(Header.engineering == False).filter(Header.science_verification == False)

        # Limit to things within 1 year
        now = datetime.datetime.now()
        year = timedelta(days=366)
        then = now - year
        query = query.filter(Header.ut_datetime > then)

        query = query.order_by(desc(Header.observation_id), desc(Header.ut_datetime))

        # OK, we're going to build the results table as a list of dictionaries first, so that we can group the obsIDs together
        # when we display the HTML.

        NAndD = namedtuple("NAndD", "observation_id count young age")

        done = set()

        for l in query:
            c = get_cal_object(session, None, header=l)
            darks = c.dark()
            young = 0
            oldest = 0
            count = 0
            oldest = 0
            for d in darks:
                count += 1
                # For each dark, figure out the time difference
                age = interval_hours(l, d)
                if age < 4320:
                    young += 1
                if fabs(age) > fabs(oldest):
                    oldest = age

            entry = NAndD(l.observation_id, count, young, int(round(oldest/720, 1)))
            if entry not in done:
                done.add(entry)
                yield entry

    result.update(dict(
        twilight        = sorted([(k, tuple(v)) for (k, v) in list(twilight.items())], reverse=True),
        bias            = sorted(list(bias.items()), reverse=True),
        total_bias      = total_bias,
        binlist         = ('1x1', '2x2', '2x1', '1x2', '2x4', '4x2', '4x1', '1x4', '4x4'),
        roilist         = ('Full Frame', 'Central Spectrum'),
        nobiases        = nobiases,
        nod_and_shuffle = nod_and_shuffle(selection),
        datething       = datething
        ))

    return result
