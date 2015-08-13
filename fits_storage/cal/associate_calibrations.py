"""
This module contains the "associated calibrations" code. It is used
to generate a summary table of calibration data associated with the
results of a search
"""
from . import get_cal_object
from ..gemini_metadata_utils import cal_types
from ..orm.header import Header
from ..orm.calcache import CalCache

mapping = {
    'processed_bias': ('bias', { 'processed': True }),
    'processed_flat': ('flat', { 'processed': True }),
    }

def associate_cals(session, headers, caltype="all", recurse_level=0):
    """
    This function takes a list of headers from a search result and
    generates a list of the associated calibration headers
    We return a priority ordered (best first) list
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
    # Only necessary if we looked at multiple headers
    ids = set()
    if len(headers) > 1:
        shortlist = []
        for calheader in calheaders:
            if calheader.id not in ids:
                ids.add(calheader.id)
                shortlist.append(calheader)
    else:
        shortlist = calheaders
        ids.add(calheaders[0].id)

    # Now we have to recurse to find the calibrations for the calibrations...
    # We only do this for caltype all.
    # Keep digging deeper until we don't find any extras, or we hit too many recurse levels

    if caltype == 'all' and recurse_level < 4 and len(shortlist) > 0:
        for cal in associate_cals(session, shortlist, caltype=caltype, recurse_level=recurse_level + 1):
            if cal.id not in ids:
                shortlist.append(cal)

    # All done, return the shortlist
    return shortlist

def associate_cals_from_cache(session, headers, caltype="all", recurse_level=0):
    """
    This function takes a list of headers from a search result and
    generates a list of the associated calibration headers
    We return a priority ordered (best first) list

    This is the same interface as associate_cals above, but this version
    queries the CalCache table rather than actually doing the association
    """

    calheaders = []

    # We can do this a bit more efficiently than the non-cache version, as we can do one
    # big 'distinct' query rather than de-duplicating after the fact.

    # Make a list of the obs_hids
    obs_hids = []
    for header in headers:
        obs_hids.append(header.id)

    query = session.query(CalCache.cal_hid).filter(CalCache.obs_hid.in_(obs_hids))
    if caltype != 'all':
        query = query.filter(CalCache.caltype == caltype)
    query = query.distinct().order_by(CalCache.caltype).order_by(CalCache.rank)

    # for result in query.all():
    #     calheader = session.query(Header).filter(Header.id == result[0]).one()
    #     calheaders.append(calheader)

    calheaders = session.query(Header).filter(Header.id.in_([res[0] for res in query]))
    ids = set(calh.id for calh in calheaders)

    # Now we have to recurse to find the calibrations for the calibrations...
    # We only do this for caltype all.
    # Keep digging deeper until we don't find any extras, or we hit too many recurse levels

    if caltype == 'all' and recurse_level < 4 and len(calheaders) > 0:
        for cal in associate_cals_from_cache(session, calheaders, caltype=caltype, recurse_level=recurse_level + 1):
            if cal.id not in ids:
                calheaders.append(cal)

    return calheaders

