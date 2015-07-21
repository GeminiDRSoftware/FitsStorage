"""
This module contains the "associated calibrations" code. It is used
to generate a summary table of calibration data associated with the
results of a search
"""
from cal import get_cal_object
from orm.header import Header
from orm.calcache import CalCache

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
        if 'arc' in calobj.applicable and (caltype == 'all' or caltype == 'arc'):
            arcs = calobj.arc()
            if arcs:
                calheaders += arcs

        if 'dark' in calobj.applicable and (caltype == 'all' or caltype == 'dark'):
            darks = calobj.dark()
            if darks:
                calheaders += darks

        if 'bias' in calobj.applicable and (caltype == 'all' or caltype == 'bias'):
            biases = calobj.bias()
            if biases:
                calheaders += biases

        if 'flat' in calobj.applicable and (caltype == 'all' or caltype == 'flat'):
            flats = calobj.flat()
            if flats:
                calheaders += flats

        if 'domeflat' in calobj.applicable and (caltype == 'all' or caltype == 'domeflat'):
            domeflats = calobj.domeflat()
            if domeflats:
                calheaders += domeflats

        if 'lampoff_flat' in calobj.applicable and (caltype == 'all' or caltype == 'lampoff_flat'):
            lampoff_flats = calobj.lampoff_flat()
            if lampoff_flats:
                calheaders += lampoff_flats

        if 'qh_flat' in calobj.applicable and (caltype == 'all' or caltype == 'qh_flat'):
            qh_flats = calobj.lampoff_flat()
            if qh_flats:
                calheaders += qh_flats

        if 'processed_bias' in calobj.applicable and (caltype == 'all' or caltype == 'processed_bias'):
            processed_biases = calobj.bias(processed=True)
            if processed_biases:
                calheaders += processed_biases

        if 'processed_flat' in calobj.applicable and (caltype == 'all' or caltype == 'processed_flat'):
            processed_flats = calobj.flat(processed=True)
            if processed_flats:
                calheaders += processed_flats

        if 'processed_fringe' in calobj.applicable and (caltype == 'all' or caltype == 'processed_fringe'):
            processed_fringes = calobj.processed_fringe()
            if processed_fringes:
                calheaders += processed_fringes

        if 'pinhole_mask' in calobj.applicable and (caltype == 'all' or caltype == 'pinhole_mask'):
            pinhole_masks = calobj.pinhole_mask()
            if pinhole_masks:
                calheaders += pinhole_masks

        if 'ronchi_mask' in calobj.applicable and (caltype == 'all' or caltype == 'ronchi_mask'):
            ronchi_masks = calobj.ronchi_mask()
            if ronchi_masks:
                calheaders += ronchi_masks

        if 'spectwilight' in calobj.applicable and (caltype == 'all' or caltype == 'spectwilight'):
            spectwilights = calobj.spectwilight()
            if spectwilights:
                calheaders += spectwilights

        if 'specphot' in calobj.applicable and (caltype == 'all' or caltype == 'specphot'):
            specphots = calobj.specphot()
            if specphots:
                calheaders += specphots

        if 'photometric_standard' in calobj.applicable and (caltype == 'all' or caltype == 'photometric_standard'):
            photometric_standards = calobj.photometric_standard()
            if photometric_standards:
                calheaders += photometric_standards

        if 'telluric_standard' in calobj.applicable and (caltype == 'all' or caltype == 'telluric_standard'):
            telluric_standards = calobj.telluric_standard()
            if telluric_standards:
                calheaders += telluric_standards

    # Now loop through the calheaders list and remove duplicates.
    # Only necessary if we looked at multiple headers
    if len(headers) > 1:
        shortlist = []
        ids = []
        for calheader in calheaders:
            if calheader.id not in ids:
                ids.append(calheader.id)
                shortlist.append(calheader)
    else:
        shortlist = calheaders


    # Now we have to recurse to find the calibrations for the calibrations...
    # We only do this for caltype all.
    # Keep digging deeper until we don't find any extras, or we hit too many recurse levels

    if caltype == 'all':
        recurse_level += 1
        if recurse_level < 5 and len(shortlist) > 0:
            cals_cals = associate_cals(session, shortlist, caltype=caltype, recurse_level=recurse_level)
            for cal in cals_cals:
                if cal not in shortlist:
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

    for result in query.all():
        calheader = session.query(Header).filter(Header.id == result[0]).one()
        calheaders.append(calheader)

    # Now we have to recurse to find the calibrations for the calibrations...
    # We only do this for caltype all.
    # Keep digging deeper until we don't find any extras, or we hit too many recurse levels

    if caltype == 'all':
        recurse_level += 1
        if recurse_level < 5 and len(calheaders) > 0:
            cals_cals = associate_cals_from_cache(session, calheaders, caltype=caltype, recurse_level=recurse_level)
            for cal in cals_cals:
                if cal not in calheaders:
                    calheaders.append(cal)


    return calheaders

