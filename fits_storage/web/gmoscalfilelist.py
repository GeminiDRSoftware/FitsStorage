"""
This module contains the gmoscal html generator function.
"""
import sqlalchemy
from sqlalchemy.sql.expression import cast
from sqlalchemy import func, join, desc
from ..orm.gmos import Gmos
from ..orm.header import Header
from ..orm.diskfile import DiskFile
from ..orm.file import File

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


@templating.templated("gmoscalfiles.json")
def gmoscalfiles():
    """
    This generates a GMOS imaging twilight flat, bias and nod and shuffle darks file list.
    """
    fromdt = datetime.date.today() - timedelta(days=180)

    result = dict(
        file_list = list(),
        is_development = fits_system_status == 'development',
    )

    if using_sqlite:
        result['using_sqlite'] = True
        return Return.HTTP_NOT_IMPLEMENTED, result

    session = get_context().session

    rs = session.execute("""
        with last_processed as (
            select max(ph.ut_datetime) as dt, 
                   ph.filter_name as filter, 
                   ph.detector_binning as binning 
            from header ph, diskfile df
            where ph.instrument in ('GMOS-N', 'GMOS-S') 
                and ph.ut_datetime > :dt 
                and ph.types like '%PREPARED%' 
                and ph.observation_class='dayCal' 
                and ph.object='Twilight' 
                and ph.detector_roi_setting='Full Frame' 
                and ph.mode='imaging'
                and ph.diskfile_id=df.id
                and df.filename like '%_flat.fits'
                and df.canonical
            group by ph.filter_name, ph.detector_binning
        )
        select count(1) as num, h.observation_class, h.filter_name, h.detector_binning, last_processed.dt 
        from header h
        join last_processed on h.ut_datetime>=(date(last_processed.dt) + INTERVAL '1 day') 
        and h.instrument in ('GMOS-N', 'GMOS-S') 
        and h.filter_name=last_processed.filter
        and h.detector_binning=last_processed.binning
        and (h.qa_state='Pass' or (h.qa_state='Undefined' and h.observation_class='science'))
        join diskfile df on h.diskfile_id=df.id
        where df.canonical and h.observation_class in ('science', 'dayCal')
        and (h.observation_class='science' or (h.object='Twilight' and h.detector_roi_setting='Full Frame'))
        group by h.observation_class, h.filter_name, h.detector_binning, last_processed.dt
    """, {"dt": fromdt})

    counts = dict()
    for row in rs:
        num = row["num"]
        clazz = row["observation_class"]
        filter = row["filter_name"]
        bin = row["detector_binning"]
        dt = row["dt"]

        key = "%s_%s" % (filter, bin)
        if key not in counts:
            counts[key] = {"science": 0, "twilights": 0, "filter": filter, "bin": bin, "dt": dt.strftime('%Y-%m-%d'),}
        dat = counts[key]
        if clazz == "science":
            dat["science"] = num
        else:
            dat["twilights"] = num

    twilight_filenames = dict()
    for filter_name in [
        'DS920',
        'g',
        'GG455&g',
        'Ha',
        'HaC',
        'HeII',
        'HeIIC',
        'i',
        'OG515&g',
        'OIII',
        'OIIIC',
        'OVI',
        'OVIC',
        'q',
        'r',
        'ri',
        'SII',
        'Y',
        'z',
        'Z'
    ]:
        for detector_binning in [
            '1x1',
            '2x2',
            '4x4'
        ]:
            key = "%s_%s" % (filter_name, detector_binning)
            if key not in counts.keys():
                dt = fromdt
            else:
                dt = counts[key]['dt']
            rs = session.execute("""
                select df.filename
                from header h, diskfile df
                where df.canonical and h.diskfile_id=df.id 
                and h.ut_datetime>=:dt and h.instrument in ('GMOS-N', 'GMOS-S') 
                and h.filter_name=:filter_name
                and h.detector_binning=:detector_binning
                and h.observation_class = 'dayCal'
                and h.qa_state='Pass'
                and (h.object='Twilight' and h.detector_roi_setting='Full Frame')
            """, {"dt": dt, "filter_name": filter_name, "detector_binning": detector_binning})
            filenames_list = list()
            for row in rs:
                filenames_list.append(row["filename"])
            if filenames_list:
                twilight_filenames[key] = filenames_list

    # Now we do the biases in a similar manner
    # TODO refactor with the above to pull out the common stuff
    rs = session.execute("""
        with last_processed as (
            select max(ph.ut_datetime) as dt, 
                   ph.filter_name as filter, 
                   ph.detector_binning as binning 
            from header ph, diskfile df
            where ph.instrument in ('GMOS-N', 'GMOS-S') 
                and ph.ut_datetime > :dt 
                and ph.types like '%PREPARED%' 
                and ph.observation_class='dayCal' 
                and ph.object='Bias' 
                and ph.detector_roi_setting='Full Frame' 
                and ph.mode='imaging'
                and ph.diskfile_id=df.id
                and df.filename like '%_bias.fits'
                and df.canonical
            group by ph.filter_name, ph.detector_binning
        )
        select count(1) as num, h.observation_class, h.filter_name, h.detector_binning, last_processed.dt 
        from header h
        join last_processed on h.ut_datetime>=(date(last_processed.dt) + INTERVAL '1 day') 
        and h.instrument in ('GMOS-N', 'GMOS-S') 
        and h.filter_name=last_processed.filter
        and h.detector_binning=last_processed.binning
        and (h.qa_state='Pass' or (h.qa_state='Undefined' and h.observation_class='science'))
        join diskfile df on h.diskfile_id=df.id
        where df.canonical and h.observation_class in ('science', 'dayCal')
        and (h.observation_class='science' or (h.object='Bias' and h.detector_roi_setting='Full Frame'))
        group by h.observation_class, h.filter_name, h.detector_binning, last_processed.dt
    """, {"dt": fromdt})

    counts = dict()
    for row in rs:
        num = row["num"]
        clazz = row["observation_class"]
        filter = row["filter_name"]
        bin = row["detector_binning"]
        dt = row["dt"]

        key = "%s_%s" % (filter, bin)
        if key not in counts:
            counts[key] = {"science": 0, "biases": 0, "filter": filter, "bin": bin, "dt": dt.strftime('%Y-%m-%d'),}
        dat = counts[key]
        if clazz == "science":
            dat["science"] = num
        else:
            dat["biases"] = num

    bias_filenames = dict()
    for filter_name in [
        'DS920',
        'g',
        'GG455&g',
        'Ha',
        'HaC',
        'HeII',
        'HeIIC',
        'i',
        'OG515&g',
        'OIII',
        'OIIIC',
        'OVI',
        'OVIC',
        'q',
        'r',
        'ri',
        'SII',
        'Y',
        'z',
        'Z'
    ]:
        for detector_binning in [
            '1x1',
            '2x2',
            '4x4'
        ]:
            key = "%s_%s" % (filter_name, detector_binning)
            if key not in counts.keys():
                dt = fromdt
            else:
                dt = counts[key]['dt']
            rs = session.execute("""
                select df.filename
                from header h, diskfile df
                where df.canonical and h.diskfile_id=df.id 
                and h.ut_datetime>=:dt and h.instrument in ('GMOS-N', 'GMOS-S') 
                and h.filter_name=:filter_name
                and h.detector_binning=:detector_binning
                and h.observation_class = 'dayCal'
                and h.qa_state='Pass'
                and (h.object='Bias' and h.detector_roi_setting='Full Frame')
            """, {"dt": dt, "filter_name": filter_name, "detector_binning": detector_binning})
            filenames_list = list()
            for row in rs:
                filenames_list.append(row["filename"])
            if filenames_list:
                bias_filenames[key] = filenames_list

    result.update(dict(
        file_list=json.dumps({'twilight': twilight_filenames, 'bias': bias_filenames}),
        ))

    return result
