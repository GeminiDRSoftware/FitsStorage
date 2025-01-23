from sqlalchemy import select

from fits_storage.cal.calibration import get_cal_object
from fits_storage.gemini_metadata_utils import cal_types
from fits_storage.core.orm.header import Header
from fits_storage.cal.orm.calcache import CalCache


def associate_cals(session, headers, caltype="all", recurse_level=0):
    """
    This function takes a list of headers and returns a priority ordered list
    of the associated calibration headers. Note, this is used by the
    "View Associated Calibrations" web function and in building the calcache.
    It is not used directly by the calmgr calibration manager API

    Parameters
    ----------

    session : :class:`~sqlalchemy.orm.Session`
        The open session to use for querying data

    headers : list of :class:`Header`
        A list of headers to get the appropriate calibration objects for

    caltype : str, defaults to "all"
        Type of calibration to lookup, or "all" for all types

    recurse_level : int, defaults to 0
        The current depth of the query, should initially be passed in as 0.

    Returns
    -------

    list of :class:`Header` calibration records

    """

    calheaders = []

    for header in headers:
        # Get a calibration object on this science header
        calobj = get_cal_object(session, None, header=header)

        # Go through the calibration types. The processed_cal types are listed
        # explicitly in the cal_types list - for those we need to call
        # cal(processed=True).
        for ct in cal_types:
            if ct in calobj.applicable and (caltype == 'all' or caltype == ct):
                if ct.startswith('processed_'):
                    newcals = getattr(calobj, ct[10:])(processed=True)
                else:
                    newcals = getattr(calobj, ct)()
                calheaders.extend(newcals)

    # Now loop through the calheaders list and remove duplicates.
    ids = set()
    shortlist = []
    for calheader in calheaders:
        if calheader.id not in ids:
            ids.add(calheader.id)

            # Need to check if it's already set so that primary cals that are
            # also secondary cals stay as primary.
            if not hasattr(calheader, 'is_primary_cal'):
                calheader.is_primary_cal = recurse_level == 0
            shortlist.append(calheader)
    calheaders = shortlist

    # Now we have to recurse to find the calibrations for the calibrations...
    # We only do this for caltype all. Keep digging deeper until we don't
    # find any extras, or we hit too many recurse levels

    if caltype == 'all' and recurse_level < 5 and len(calheaders) > 0:
        morecals = associate_cals(session, calheaders, caltype=caltype,
                                  recurse_level=recurse_level + 1)
        for cal in morecals:
            if cal.id not in ids:
                calheaders.append(cal)

    # All done, return the calheaders
    return calheaders


def associate_cals_from_cache(session, headers, caltype="all", recurse_level=0):
    """
    This function takes a list of :class:`fits_storage.orm.header.Header`
    from a search result and generates a list of the associated calibration
    :class:`fits_storage.orm.header.Header` We return a priority ordered
    (best first) list

    This is the same interface as associate_cals above, but this version
    queries the :class:`~CalCache` table rather
    than actually doing the association.

    Parameters
    ----------

    session : :class:`sqlalchemy.orm.Session`
        The open session to use for querying data

    headers : list of :class:`Header`
        A list of headers to get the appropriate calibration objects for

    caltype : str, defaults to "all"
        Type of calibration to lookup, or "all" for all types

    recurse_level : int, defaults to 0
        The current depth of the query, should initally be passed in as 0.

    Returns
    -------

    list of :class:`Header` calibration records

    """
    # We can do this a bit more efficiently than the non-cache version,
    # as we can do one big 'distinct' query rather than de-duplicating after
    # the fact.

    # Make a list of the obs_hids
    obs_hids = []
    for header in headers:
        obs_hids.append(header.id)

    stmt = select(Header, CalCache.caltype, CalCache.rank)\
        .join(CalCache, CalCache.cal_hid == Header.id)\
        .where(CalCache.obs_hid.in_(obs_hids))

    if caltype != 'all':
        stmt = stmt.where(CalCache.caltype == caltype)

    stmt = stmt.distinct().order_by(CalCache.caltype).order_by(CalCache.rank)

    calheaders = []
    calhids = []

    for calhead in session.scalars(stmt):
        calhids.append(calhead.id)
        calheaders.append(calhead)
        # Need to check if it's already set so that primary cals that are
        # also secondary cals stay as primary.
        if not hasattr(calhead, 'is_primary_cal'):
            calhead.is_primary_cal = recurse_level == 0

    # Now we have to recurse to find the calibrations for the calibrations...
    # We only do this for caltype all. Keep digging deeper until we don't
    # find any extras, or we hit too many recurse levels

    if caltype == 'all' and recurse_level < 4 and len(calheaders) > 0:
        down_list = calheaders
        for cal in associate_cals_from_cache(session, down_list,
                                             caltype=caltype,
                                             recurse_level=recurse_level + 1):
            if cal.id not in calhids:
                calhids.append(cal.id)
                calheaders.append(cal)

    return calheaders
