"""
This module contains the "associated calibrations" code. It is used
to generate a summary table of calibration data associated with the
results of a search
"""
from fits_storage.cal.calibration import get_cal_object
from fits_storage.gemini_metadata_utils import cal_types
from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header
from fits_storage.cal.orm.calcache import CalCache

mapping = {
    'processed_bias': ('bias', {'processed': True}),
    'processed_flat': ('flat', {'processed': True}),
    'processed_arc': ('arc', {'processed': True}),
    'processed_dark': ('dark', {'processed': True}),
    'processed_standard': ('standard', {'processed': True}),
    'processed_slitillum': ('slitillum', {'processed': True}),
    'processed_bpm': ('bpm', {'processed': True}),
    }


def associate_cals(session, headers, caltype="all", recurse_level=0):
    """
    This function takes a list of headers from a search result and
    generates a list of the associated calibration headers
    We return a priority ordered (best first) list

    Parameters
    ----------

    session : :class:`~sqlalchemy.orm.Session`
        The open session to use for querying data


    header : list of :class:`~gemini_obs_db.orm.header.Header`
        A list of headers to get the appropriate calibration objects for

    caltype : str, defaults to "all"
        Type of calibration to lookup, or "all" for all types

    recurse_level : int, defaults to 0
        The current depth of the query, should initally be passed in as 0.

    Returns
    -------

    list of :class:`~gemini_obs_db.orm.header.Header` calibration records or,

    """

    calheaders = []

    for header in headers:
        # Get a calibration object on this science header
        calobj = get_cal_object(session, None, header=header)

        # Go through the calibration types. For now we just look for both
        # raw and processed versions of each.
        for ct in cal_types:
            if ct in calobj.applicable and (caltype == 'all' or caltype == ct):
                mapped_name, mapped_args = mapping.get(ct, (ct, None))
                if mapped_args is None:
                    calheaders.extend(getattr(calobj, ct)())
                else:
                    calheaders.extend(getattr(calobj, mapped_name)(**mapped_args))

    # Now loop through the calheaders list and remove duplicates.
    ids = set()
    shortlist = []
    for calheader in calheaders:
        if calheader.id not in ids:
            ids.add(calheader.id)
            shortlist.append(calheader)

    # Now we have to recurse to find the calibrations for the calibrations...
    # We only do this for caltype all. Keep digging deeper until we don't
    # find any extras, or we hit too many recurse levels

    if caltype == 'all' and recurse_level < 1 and len(shortlist) > 0:
        down_list = shortlist
        for cal in associate_cals(session, down_list, caltype=caltype, recurse_level=recurse_level + 1):
            if cal.id not in ids:
                if recurse_level == 0:
                    cal.is_primary_cal = True
                else:
                    cal.is_primary_cal = False
                shortlist.append(cal)

    def sort_cal_fn(a):
        try:
            return "BPM" if a is not None and a[0].observation_type == "BPM" else "X"
        except:
            return "X"

    if recurse_level == 0:
        shortlist.sort(key=sort_cal_fn)

    # All done, return the shortlist
    return shortlist


# TODO does this get called any more?!
def associate_cals_from_cache(session, headers, caltype="all", recurse_level=0):
    """
    This function takes a list of :class:`fits_storage.orm.header.Header`
    from a search result and generates a list of the associated calibration
    :class:`fits_storage.orm.header.Header` We return a priority ordered (
    best first) list

    This is the same interface as associate_cals above, but this version
    queries the :class:`~gemini_obs_db.orm.calcache.CalCache` table rather
    than actually doing the association.

    Parameters
    ----------

    session : :class:`sqlalchemy.orm.Session`
        The open session to use for querying data

    headers : list of :class:`gemini_obs_db.orm.header.Header`
        A list of headers to get the appropriate calibration objects for

    caltype : str, defaults to "all"
        Type of calibration to lookup, or "all" for all types

    recurse_level : int, defaults to 0
        The current depth of the query, should initally be passed in as 0.

    Returns
    -------

    list of :class:`~gemini_obs_db.orm.header.Header` calibration records

    """
    # We can do this a bit more efficiently than the non-cache version,
    # as we can do one big 'distinct' query rather than de-duplicating after
    # the fact.

    # Make a list of the obs_hids
    obs_hids = []
    for header in headers:
        obs_hids.append(header.id)

    query = session.query(Header).join(CalCache, Header.id == CalCache.cal_hid)
    query = query.filter(CalCache.obs_hid.in_(obs_hids))
    if caltype != 'all':
        query = query.filter(CalCache.caltype == caltype)
    query = query.distinct().order_by(CalCache.caltype).\
        order_by(CalCache.obs_hid).order_by(CalCache.rank)

    calheaders = query.all()
    # for cal in calheaders:
    #     cal.is_primary_cal = True
    ids = set(calh.id for calh in calheaders)

    # Now we have to recurse to find the calibrations for the calibrations...
    # We only do this for caltype all. Keep digging deeper until we don't
    # find any extras, or we hit too many recurse levels

    if caltype == 'all' and recurse_level < 4 and len(calheaders) > 0:
        down_list = calheaders
        for cal in associate_cals_from_cache(session, down_list, caltype=caltype, recurse_level=recurse_level + 1):
            if cal.id not in ids:
                # cal.is_primary_cal = False
                calheaders.append(cal)

    def sort_cal_fn(a):
        return "BPM" if a is not None and len(a) > 0 and a[0].observation_type == "BPM" else "X"

    if recurse_level == 0:
        calheaders.sort(key=sort_cal_fn)

    return calheaders
