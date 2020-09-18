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

@templating.templated("gmoscaltwilightdetails.html")
def gmoscaltwilightdetails():
    """
    This generates a GMOS imaging twilight flat, bias and nod and shuffle darks report.
    If no date or daterange is given, tries to find last processing date
    """

    result = dict(
        is_development = fits_system_status == 'development',
        )

    if using_sqlite:
        result['using_sqlite'] = True
        return Return.HTTP_NOT_IMPLEMENTED, result

    session = get_context().session

    fromdt = datetime.datetime.now() - timedelta(days=180)
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
            group by ph.filter_name, ph.detector_binning
        )
        select count(1) as num, h.observation_class, h.filter_name, h.detector_binning, last_processed.dt 
        from header h 
        join last_processed on h.ut_datetime>=last_processed.dt and h.instrument in ('GMOS-N', 'GMOS-S') 
        and h.filter_name=last_processed.filter
        and h.detector_binning=last_processed.binning
        where h.observation_class in ('science', 'dayCal')
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
        key = "%s-%s" % (filter, bin)
        if key not in counts:
            counts[key] = {"science": 0, "twilights": 0, "filter": filter, "bin": bin, "dt": dt}
        dat = counts[key]
        if clazz == "science":
            dat["science"] = num
        else:
            dat["twilights"] = num

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
            key = "%s-%s" % (filter_name, detector_binning)
            if key not in counts.keys():
                counts[key] = {"science": 0, "twilights": 0, "filter": filter_name, "bin": detector_binning, "dt": fromdt}
                rs = session.execute("""
                    select count(1) as num, h.observation_class, :dt 
                    from header h 
                    where h.ut_datetime>=:dt and h.instrument in ('GMOS-N', 'GMOS-S') 
                    and h.filter_name=:filter_name
                    and h.detector_binning=:detector_binning
                    and h.observation_class in ('science', 'dayCal')
                    and (h.observation_class='science' or (h.object='Twilight' and h.detector_roi_setting='Full Frame'))
                    group by h.observation_class, h.filter_name, h.detector_binning
                """, {"dt": fromdt, "filter_name": filter_name, "detector_binning": detector_binning})
                for row in rs:
                    num = row["num"]
                    clazz = row["observation_class"]
                    dat = counts[key]
                    if clazz == "science":
                        dat["science"] = num
                    else:
                        dat["twilights"] = num

    result.update(dict(
        counts=sorted(list(counts.values()), key=lambda x: "%s-%s" % (x["filter"], x["bin"])),
        ))

    return result
