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


# Set this True to enable cache.  Adam Smith says we want the results to be live after all
_cache_results = False
_cached_twilight_results = None
_cached_twilight_date = None


@templating.templated("gmoscaltwilightdetails.html")
def gmoscaltwilightdetails():
    """
    This generates a GMOS imaging twilight flat, bias and nod and shuffle darks report.
    If no date or daterange is given, tries to find last processing date
    """

    # -- check our cached results
    # if we ever come up with a single query for the below, may consider materialized view (also after we upgrade
    # all servers to CentOS 8)
    global _cached_twilight_date
    global _cached_twilight_results

    fromdt = datetime.date.today() - timedelta(days=180)

    if _cached_twilight_results is not None and _cached_twilight_date is not None and _cached_twilight_date == fromdt:
        return _cached_twilight_results

    _cached_twilight_date = fromdt
    _cached_twilight_results = None
    # -- end cache check

    result = dict(
        is_development = fits_system_status == 'development',
        )

    if using_sqlite:
        result['using_sqlite'] = True
        return Return.HTTP_NOT_IMPLEMENTED, result

    session = get_context().session

    # rs = session.execute("""
    #     with last_processed as (
    #         select max(ph.ut_datetime) as dt,
    #                ph.filter_name as filter,
    #                ph.detector_binning as binning
    #         from header ph, diskfile df
    #         where ph.instrument in ('GMOS-N', 'GMOS-S')
    #             and ph.ut_datetime > :dt
    #             and ph.types like '%PREPARED%'
    #             and ph.observation_class='dayCal'
    #             and ph.object='Twilight'
    #             and ph.detector_roi_setting='Full Frame'
    #             and ph.mode='imaging'
    #             and ph.diskfile_id=df.id
    #             and df.filename like '%_flat.fits'
    #             and df.canonical
    #         group by ph.filter_name, ph.detector_binning
    #     )
    #     select count(1) as num, h.observation_class, h.filter_name, h.detector_binning, last_processed.dt
    #     from header h
    #     join last_processed on h.ut_datetime>=(date(last_processed.dt) + INTERVAL '1 day')
    #     and h.instrument in ('GMOS-N', 'GMOS-S')
    #     and h.filter_name=last_processed.filter
    #     and h.detector_binning=last_processed.binning
    #     and (h.qa_state='Pass' or (h.qa_state='Undefined' and h.observation_class='science'))
    #     join diskfile df on h.diskfile_id=df.id
    #     where df.canonical and h.observation_class in ('science', 'dayCal')
    #     and (h.observation_class='science' or (h.object='Twilight' and h.detector_roi_setting='Full Frame'))
    #     group by h.observation_class, h.filter_name, h.detector_binning, last_processed.dt
    # """, {"dt": fromdt})

    # rs = session.execute("""
    #     with last_processed as (
    #         select max(ph.ut_datetime) as dt,
    #                ph.filter_name as filter,
    #                ph.detector_binning as binning
    #         from header ph, diskfile df
    #         where ph.instrument in ('GMOS-N', 'GMOS-S')
    #             and ph.ut_datetime > :dt
    #             and ph.types like '%PREPARED%'
    #             and ph.observation_class='dayCal'
    #             and ph.object='Twilight'
    #             and ph.detector_roi_setting='Full Frame'
    #             and ph.mode='imaging'
    #             and ph.diskfile_id=df.id
    #             and df.filename like '%_flat.fits'
    #             and df.canonical
    #         group by ph.filter_name, ph.detector_binning
    #     )
    #     select count(1) as num, h.observation_class, last_processed.filter, last_processed.binning, last_processed.dt
    #     from last_processed
    #     left outer join header h on h.ut_datetime>=(date(last_processed.dt) + INTERVAL '1 day')
    #     and h.instrument in ('GMOS-N', 'GMOS-S')
    #     and h.filter_name=last_processed.filter
    #     and h.detector_binning=last_processed.binning
    #     and (h.qa_state='Pass' or (h.qa_state='Undefined' and h.observation_class='science'))
    #     and (h.observation_class='science' or (h.object='Twilight' and h.detector_roi_setting='Full Frame'))
    #     and h.observation_class in ('science', 'dayCal')
    #     left outer join diskfile df on h.diskfile_id=df.id
    #     and df.canonical
    #     group by h.observation_class, last_processed.filter, last_processed.binning, last_processed.dt
    # """, {"dt": fromdt})

    rs = session.execute("""
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
    """, {"dt": fromdt})

    counts = dict()
    for row in rs:
        dt = row["dt"]
        filter = row["filter"]
        binning = row["binning"]

        key = "%s-%s" % (filter, binning)
        counts[key] = {"science": 0, "twilights": 0, "filter": filter, "bin": binning,
                       "dt": dt.strftime('%Y-%m-%d'),
                       "filename": "none"}

        if dt != fromdt:
            # fetch the filename
            filename_rs = session.execute("""
                select df.filename
                from header h, diskfile df
                where df.canonical and h.diskfile_id=df.id
                    and h.instrument in ('GMOS-N', 'GMOS-S')
                    and h.ut_datetime = :dt
                    and h.types like '%PREPARED%'
                    and h.observation_class='dayCal'
                    and h.object = 'Twilight'
                    and h.detector_roi_setting='Full Frame'
                    and h.mode='imaging'
                    and df.filename like '%_flat.fits'
                    and h.filter_name=:filter_name
                    and h.detector_binning=:detector_binning
            """, {"dt": dt, "filter_name": filter, "detector_binning": binning})
            filename_row = filename_rs.fetchone()
            if filename_row is not None:
                counts[key]["filename"] = filename_row["filename"]

        rs2 = session.execute("""
            select count(1) as num, h.observation_class
            from header h, diskfile df where h.ut_datetime>=(date(:dt) + INTERVAL '1 day') 
            and h.instrument in ('GMOS-N', 'GMOS-S') 
            and h.filter_name=:filter
            and h.detector_binning=:binning
            and (h.qa_state='Pass' or (h.qa_state='Undefined' and h.observation_class='science'))
            and (h.observation_class='science' or (h.object='Twilight' and h.detector_roi_setting='Full Frame'))
            and h.observation_class in ('science', 'dayCal')
            and h.diskfile_id=df.id
            and df.canonical
            and h.observation_type='OBJECT'
            and h.spectroscopy=False
            and h.engineering=False
            and h.science_verification=False
            group by h.observation_class
            """, {"dt": dt, "filter": filter, "binning": binning})
        for row2 in rs2:
            num = row2["num"]
            clazz = row2["observation_class"]
            # filter = row["filter"]
            # bin = row["binning"]
            # dt = row["dt"]
            dat = counts[key]
            if clazz == "science":
                dat["science"] = num
            elif clazz == 'dayCal':
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
                rs = session.execute("""
                    select count(1) as num, h.observation_class, :dt 
                    from header h, diskfile df
                    where df.canonical and h.diskfile_id=df.id 
                    and h.ut_datetime>=:dt and h.instrument in ('GMOS-N', 'GMOS-S') 
                    and h.filter_name=:filter_name
                    and h.detector_binning=:detector_binning
                    and h.observation_class in ('science', 'dayCal')
                    and (h.qa_state='Pass' or (h.qa_state='Undefined' and h.observation_class='science'))
                    and (h.observation_class='science' or (h.object='Twilight' and h.detector_roi_setting='Full Frame'))
                    group by h.observation_class, h.filter_name, h.detector_binning
                """, {"dt": fromdt, "filter_name": filter_name, "detector_binning": detector_binning})
                for row in rs:
                    if key not in counts.keys():
                        counts[key] = {"science": 0, "twilights": 0, "filter": filter_name, "bin": detector_binning,
                                       "dt": fromdt.strftime('%Y-%m-%d'), "filename": ""}
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

    if _cache_results:
        _cached_twilight_results = result

    return result


@templating.templated("gmoscaltwilightfiles.json")
def gmoscaltwilightfiles():
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

    # TODO this was done quick based on the stats page.  I should refactor this on master
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
        select count(1) as num, h.observation_class, last_processed.filter, last_processed.binning, last_processed.dt 
        from last_processed 
        left outer join header h on h.ut_datetime>=(date(last_processed.dt) + INTERVAL '1 day') 
        and h.instrument in ('GMOS-N', 'GMOS-S') 
        and h.filter_name=last_processed.filter
        and h.detector_binning=last_processed.binning
        and (h.qa_state='Pass' or (h.qa_state='Undefined' and h.observation_class='science'))
        and (h.observation_class='science' or (h.object='Twilight' and h.detector_roi_setting='Full Frame'))
        and h.observation_class in ('science', 'dayCal')
        left outer join diskfile df on h.diskfile_id=df.id
        and df.canonical
        group by h.observation_class, last_processed.filter, last_processed.binning, last_processed.dt
    """, {"dt": fromdt})

    counts = dict()
    for row in rs:
        num = row["num"]
        clazz = row["observation_class"]
        filter = row["filter"]
        bin = row["binning"]
        dt = row["dt"]

        key = "%s_%s" % (filter, bin)
        if key not in counts:
            counts[key] = {"science": 0, "twilights": 0, "filter": filter, "bin": bin, "dt": dt.strftime('%Y-%m-%d'),}
        dat = counts[key]
        if clazz == "science":
            dat["science"] = num
        else:
            dat["twilights"] = num

    filenames = dict()
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
                and df.filename not like '%_flat.fits'
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
                filenames[key] = filenames_list

    result.update(dict(
        file_list=json.dumps(filenames),
        ))

    return result
