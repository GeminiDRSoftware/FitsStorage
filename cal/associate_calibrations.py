"""
This module contains the "associated calibrations" code. It is used
to generate a summary table of calibration data associated with the
results of a search
"""
from cal import get_cal_object
from orm.header import Header
from orm.calcache import CalCache

def associate_cals(session, headers, caltype="all"):
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
            arc = calobj.arc()
            if arc:
                calheaders.append(arc)

        if 'dark' in calobj.applicable and (caltype == 'all' or caltype == 'dark'):
            dark = calobj.dark()
            if dark:
                calheaders.append(dark)

        if 'bias' in calobj.applicable and (caltype == 'all' or caltype == 'bias'):
            biases = calobj.bias(many=10)
            for bias in biases:
                calheaders.append(bias)

        if 'flat' in calobj.applicable and (caltype == 'all' or caltype == 'flat'):
            flat = calobj.flat()
            if flat:
                calheaders.append(flat)

        if 'processed_bias' in calobj.applicable and (caltype == 'all' or caltype == 'processed_bias'):
            processed_bias = calobj.bias(processed=True)
            if processed_bias:
                calheaders.append(processed_bias)

        if 'processed_flat' in calobj.applicable and (caltype == 'all' or caltype == 'processed_flat'):
            processed_flat = calobj.flat(processed=True)
            if processed_flat:
                calheaders.append(processed_flat)

        if 'processed_fringe' in calobj.applicable and (caltype == 'all' or caltype == 'processed_fringe'):
            processed_fringe = calobj.processed_fringe()
            if processed_fringe:
                calheaders.append(processed_fringe)

        if 'pinhole_mask' in calobj.applicable and (caltype == 'all' or caltype == 'pinhole_mask'):
            pinhole_mask = calobj.pinhole_mask()
            if pinhole_mask:
                calheaders.append(pinhole_mask)

        if 'ronchi_mask' in calobj.applicable and (caltype == 'all' or caltype == 'ronchi_mask'):
            ronchi_mask = calobj.ronchi_mask()
            if ronchi_mask:
                calheaders.append(ronchi_mask)

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

    # All done, return the shortlist
    return shortlist

def associate_cals_from_cache(session, headers, caltype="all"):
    """
    This function takes a list of headers from a search result and
    generates a list of the associated calibration headers
    We return a priority ordered (best first) list

    This is the same interface as associate_cals above, but this version
    queries the CalCache table rather than actually doing the association
    """

    calheaders = []

    for header in headers:
        query = session.query(CalCache.cal_hid).filter(CalCache.obs_hid == header.id)
        if caltype != 'all':
            query = query.filter(CalCache.caltype == caltype)
        query = query.order_by(CalCache.rank)

        for result in query.all():
            calheader = session.query(Header).filter(Header.id == result[0]).one()
            calheaders.append(calheader)

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

    # All done, return the shortlist
    return shortlist

