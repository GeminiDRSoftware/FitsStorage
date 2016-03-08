"""
This module generates the Observing Statistics - ie "Open Shutter" Statistic reports for Inger
"""

from .summary import list_headers
from ..gemini_metadata_utils import gemini_time_period_from_range, ONEDAY_OFFSET

from ..utils.web import get_context, with_content_type

import ephem
import dateutil.parser
import re

@with_content_type('text/plain')
def observing_statistics(selection):
    """
    This is a handler function. It unrolls a datarange to night by night and calls
    calculate_observing_statistics() on each night, and outputs the table
    """

    ctx = get_context()
    resp = ctx.resp

    session = ctx.session

    lst = []
    if 'date' in selection.keys():
        lst.append(calculate_observing_statistics(ctx, selection, debug=False))

    # Parse daterange, kludge selection
    if 'daterange' in selection.keys():
        date, enddate = gemini_time_period_from_range(selection.pop('daterange'))
    # Loop over ut nights, calling
        copysel = dict(selection)
        while (date < enddate):
            copysel['date'] = date.strftime("%Y%m%d")
            lst.append(calculate_observing_statistics(ctx, copysel, debug=False))
            date += ONEDAY_OFFSET

    # First get a list of instruments so that we can unroll the inst dicts
    insts = set()
    for l in lst:
        for i in l['T_open_instdict']:
            insts.add(i)
    insts = sorted(insts)

    # the column headings
    cols = ['UTdate', 'T_night', 'T_science', 'T_eng', 'T_fr', 'T_weather',
            'Observer', 'Operator', 'N_inst', 'Q', 'T_open', 'E_open',
            'T_science_instdict', 'T_open_instdict', 'E_open_instdict',
            'T_open_lgs', 'T_all', 'T_all_instdict', 'T_all_conddict']

    # Print the header line, unrolling inst and cond
    resp.append('# ')
    for c in cols:

        # Is it an instdict?
        if c.find("instdict") > 0:
            resp.append_iterable(c.replace('instdict', i) + ', ' for i in insts)

        # Is it a conddict?
        elif c.find("conddict") > 0:
            resp.append_iterable(c.replace('conddict', 'cond%d' % i) + ', ' for i in range(7))

        # OK, it's just normal, print it
        else:
            resp.append(c + ', ')
    resp.append('\n')


    # OK, now spool it out the list of dicts, untolling inst and cond as we go
    for l in lst:
        for c in cols:

            # Is it an instdict?
            if c.find("instdict") > 0:
                for i in insts:
                    resp.append_iterable(l[c][i] for i in l[c].keys())
                    resp.append(', ')

            # Is it an conddict?
            elif c.find("conddict") > 0:
                resp.append(l[c][i] + ', ' for i in range(7))

                # It's a single value
                else:
                    resp.append(l[c] + ', ')
            resp.append('\n')

def calculate_observing_statistics(ctx, selection, debug=False):
    """
    This method calculates the observing statistics for a single night given in the selection
    It returns a big disctionary with all the requested data items in it.
    """

    resp = ctx.resp

    selection['canonical'] = True

    if debug:
        resp.append("# Calculate obsering Statistics on Selection: %s\n" % selection)


    # OK, get the full header list for the night
    hlist = list_headers(selection, ['ut_datetime'])

    # Trim to night time data only
    hlist = hlist_nighttime(hlist)

    if debug:
        resp.append("# Number of Nightime files: %d\n" % len(hlist))
        try:
            resp.append_iterable([
                "# Starting at: %s %s\n" % (hlist[0].data_label, hlist[0].ut_datetime),
                "# Ending at: %s %s\n" % (hlist[-1].data_label, hlist[-1].ut_datetime)
            ])
        except:
            pass

    # Figure out the observer and ssa names
    observer = 'Unknown'
    operator = 'Unknown'
    if len(hlist):
        diskfile_id = hlist[-1].diskfile_id
        query = ctx.session.query(FullTextHeader).filter(FullTextHeader.diskfile_id == diskfile_id)
        fth = query.first()
        text = fth.fulltext
        m = re.search("OBSERVER= '(.*)'", text)
        if m:
            observer = m.group(1)
        m = re.search("SSA         = '(.*)'", text)
        if m:
            operator = m.group(1)

    observer = observer.replace(',', ';')
    operator = operator.replace(',', ';')
    if debug:
        resp.append_iterable([
            "# Observer: %s\n" % observer,
            "# Operator: %s\n" % operator
        ])

    # Define the accumulators

    t_open = 0 # t_open = sum(expt) for frames on sky set to Pass
    t_open_inst = {} # Ditto but by instrument
    t_open_inststr = {} # Ditto as string form
    t_open_lgs = 0 # Ditto but LGS only

    t_all = 0    # t_all = sum(expt) for all data on sky
    t_all_inst = {} # Ditto but by instrument
    t_all_inststr = {}
    t_all_cond = {0:0.0, 1:0.0, 2:0.0, 3:0.0, 4:0.0, 5:0.0, 6:0.0}
    t_all_condstr = {0:'', 1:'', 2:'', 3:'', 4:'', 5:'', 6:''}


    inst_changes = 0
    instrument_periods = []
    if len(hlist):
        prev_instrument = hlist[0].instrument
        prev_endtime = approx_endtime(hlist[0])
        ip_timedelta_threshold = datetime.timedelta(seconds=900)
        ip = {'inst':hlist[0].instrument, 'start':hlist[0].ut_datetime}

    wx_changes = 0
    if len(hlist):
        prev_iq = hlist[0].raw_iq
        if (prev_iq == 20):
            prev_iq = 70
        prev_cc = hlist[0].raw_cc

    iqcc_stablegood = True

    nightlog_telescope = selection['telescope']
    nightlog_date = dateutil.parser.parse(selection['date']).date()
    # OK, walk through and accumulate statistics
    for h in hlist:
        # Set the telescope if unset still
        if nightlog_telescope is None:
            nightlog_telescope = h.telescope

        # Did we hit a weather change?
        wxc = False
        iq = h.raw_iq
        if iq == 20:
            iq = 70
        if iq != prev_iq:
            wxc = True
        if h.raw_cc != prev_cc:
            wxc = True
        if wxc:
            prev_iq = iq
            prev_cc = h.raw_cc
            wx_changes += 1
            if debug:
                resp.append("# WX change at: %s %s\n" % (h.data_label, h.ut_datetime))

        # Is it iqcc_stablegood?
        if h.raw_iq > 85 or h.raw_cc>70:
            iqcc_stablegood = False

        # Did we just see an instrument period change?
        ipc = False
        if h.instrument != prev_instrument:
            ipc = True
            #debug+="# New IP - instrument Swap at %s: %s - %s\n" % (h.data_label, h.instrument, prev_instrument)
        delta = h.ut_datetime - prev_endtime
        if delta > ip_timedelta_threshold:
            #debug+="#New IP - interval: %s %s %s\n" % (delta, h.ut_datetime, prev_endtime)
            ipc = True
        if ipc:
            ip['end'] = prev_endtime
            ip['hours'] = total_seconds(ip['end'] - ip['start']) / 3600.0
            instrument_periods.append(ip.copy())
            ip['inst'] = h.instrument
            ip['start'] = h.ut_datetime
            if debug:
                resp.append("# New IP starting at: %s %s\n" % (h.data_label, h.ut_datetime))

        exposure_time = float(h.exposure_time)
        # t_open
        if h.qa_state == 'Pass' and h.observation_type == 'OBJECT':
            t_open += exposure_time
            if h.instrument not in t_open_inst:
                t_open_inst[h.instrument] = 0
            t_open_inst[h.instrument] += exposure_time
            t_open_inststr[h.instrument] = "%.2f" % (t_open_inst[h.instrument] / 3600.0)
            if h.laser_guide_star:
                t_open_lgs += exposure_time

        # t_all
        if h.observation_type == 'OBJECT':
            t_all += exposure_time
            if h.instrument not in t_all_inst:
                t_all_inst[h.instrument] = 0
                t_all_inststr[h.instrument] = ""
            t_all_inst[h.instrument] += exposure_time
            t_all_inststr[h.instrument] = "%.2f" % (t_all_inst[h.instrument] / 3600.0)
            t_all_cond[obs_cond_bin(h)] += exposure_time
            t_all_condstr[obs_cond_bin(h)] = "%.2f" % (t_all_cond[obs_cond_bin(h)] / 3600.0)

        prev_instrument = h.instrument
        prev_endtime = approx_endtime(h)

    # Sum instrument periods
    t_instuse = {}
    for ip in instrument_periods:
        if ip['inst'] not in t_instuse:
            t_instuse[ip['inst']] = 0
        t_instuse[ip['inst']] += ip['hours']

    # Calculate night quality number. This needs input from Inger

    # Print summary
    if debug:
        resp.append_iterable([
            "# Summary: \n\n",
            "# WX changes: %d\n" % wx_changes,
            "# Instrument Periods: %d\n" % len(instrument_periods)
        ])
        resp.append_iterable("%s: %s - %s: %.2f\n" % (ip['inst'], ip['start'], ip['end'], ip['hours'])
                             for ip in instrument_periods)
        resp.append("# T_open = %.1f = %.2f\n" % (t_open, t_open/3600.0))
        resp.append_iterable("# T_open_%s = %.1f = %.2f\n" % (t, t_open_inst[t], t_open_inst[t]/3600.0)
                             for t in t_open_inst)
        resp.append("# T_all = %.1f = %.2f\n" % (t_all, t_all/3600.0))
        resp.append_iterable("# T_all_%s = %.1f = %.2f\n" % (t, t_all_inst[t], t_all_inst[t]/3600.0)
                             for t in t_all_inst)
        resp.append_iterable("# T_all_cond_%d = %.1f = %.2f\n" % (i, t_all_cond[i], t_all_cond[i]/3600.0)
                             for t in t_tall_cond)

    if debug:
        resp.append("# Nightlog Query: Date: %s - Telescope: %s" % (nightlog_date, nightlog_telescope))
    nlnd = nightlog_numbers(nightlog_date, nightlog_telescope)
    if debug:
        resp.append_iterable([
            "# Error message: %s" % nlnd['error_message'],
            "# T_night: %.2f\n" % nlnd['T_night'],
            "# T_science: %.2f\n" % nlnd['T_science'],
            "# T_eng: %.2f\n" % nlnd['T_eng'],
            "# T_fr: %.2f\n" % nlnd['T_fr'],
            "# T_weather: %.2f\n" % nlnd['T_weather']
        ])

    # Calculate t_science_inst and e_open_inst
    t_science_inst = {}
    t_science_inststr = {}
    e_open_inst = {}
    e_open_inststr = {}
    t_instuse_total = 0
    for inst in t_instuse.keys():
        t_instuse_total += t_instuse[inst]
    for inst in t_instuse.keys():
        t_science_inst[inst] =    (t_instuse[inst] / t_instuse_total) * (nlnd['T_science'] / nlnd['T_night'])
        t_science_inststr[inst] = "%.2f" % t_science_inst[inst]
        e_open_inst[inst] = (t_open_inst[inst] / 3600.0) / t_science_inst[inst]
        e_open_inststr[inst] = "%.2f" % e_open_inst[inst]

    # Construct the dictionary to return
    retary = {}

    retary['UTdate'] = selection['date']

    retary['Observer'] = observer
    retary['Operator'] = operator

    retary['T_night'] = "%.2f" % nlnd['T_night']
    retary['T_science'] = "%.2f" % nlnd['T_science']
    retary['T_eng'] = "%.2f" % nlnd['T_eng']
    retary['T_fr'] = "%.2f" % nlnd['T_fr']
    retary['T_weather'] = "%.2f" % nlnd['T_weather']

    retary['N_inst'] = "%d" % len(instrument_periods)
    retary['Q'] = "%d" % -100

    retary['T_open'] = "%.2f" % (t_open/3600.0)
    try:
        retary['E_open'] = "%.2f" % ((t_open / 3600.0)/nlnd['T_science'])
    except:
        retary['E_open'] = 'None'
    retary['T_science_instdict'] = t_science_inststr
    retary['T_open_instdict'] = t_open_inststr
    retary['E_open_instdict'] = e_open_inststr
    retary['T_open_lgs'] = "%.2f" % (t_open_lgs / 3600.0)

    retary['T_all'] = "%.2f" % (t_all/3600.0)
    retary['T_all_instdict'] = t_all_inststr
    retary['T_all_conddict'] = t_all_condstr

    return retary

def hlist_nighttime(hlist):
    """
    Given a list of header objects, returns the subset which fall in night time
    """

    # Get an example file - the first one
    eg = hlist[0]

    obs = ephem.Observer()
    if eg.telescope == 'Gemini-North':
        obs.lat = '19:49:25.7016'
        obs.long = '-155:28:08.616'
        obs.elevation = 4213
    if eg.telescope == 'Gemini-South':
        obs.lat = '-30:14:26.700'
        obs.long = '-70:44:12.096'
        obs.elevation = 2722

    obs.date = ephem.date(eg.ut_datetime)

    obs.horizon = '-12'
    getsdark = obs.next_setting(ephem.Sun(), use_center=True).datetime()
    getslight = obs.next_rising(ephem.Sun(), use_center=True).datetime()

    hlist_night = []
    for h in hlist:
        if h.ut_datetime > getsdark and h.ut_datetime < getslight:
            hlist_night.append(h)

    return hlist_night

def approx_endtime(h):
    """
    Calculates an approximate end time for a frame. ut_datetime + exposure_time + 5 seconds
    """

    start = h.ut_datetime
    interval = datetime.timedelta(seconds = int(h.exposure_time) + 5)
    end = start + interval

    return end

def obs_cond_bin(h):
    """
    Calculates the observing condition bin as specified by Inger
    """
    iq = int(h.raw_iq)
    cc = int(h.raw_cc)
    bg = int(h.raw_bg)

    if iq == 20 and cc == 50:
        return 0
    if iq == 70 and cc == 50 and bg <= 50:
        return 1
    if iq == 70 and cc == 50 and bg >= 80:
        return 2
    if iq <= 70 and cc >= 70:
        return 3
    if iq == 85 and cc == 50:
        return 4
    if iq == 85 and cc >= 70:
        return 5
    if iq == 100:
        return 6


import datetime
import time
from suds import *
from suds.client import Client, WebFault
def nightlog_numbers(utdate, telescope):
    """
    This function talks to the remedy database SOAP intervace using the suds module
    to query for the time usage numbers from the nightlog
    """

    url = "https://remedy.gemini.edu/arsys/WSDL/public/alderaan.gemini.edu/Gemini_NightLog"
    midnight = datetime.time(0, 0, 0, 0)
    dt = datetime.datetime.combine(utdate, midnight)
    t = time.mktime(dt.timetuple())

    # This code comes from Tom Cumming via Email.
    client = Client(url);
    token = client.factory.create('AuthenticationInfo')
    token.userName = 'gea'
    token.password = 'gea99'
    client.set_options(soapheaders=token)

    if telescope == 'Gemini-North' :
        telescope = 'North'
    elif telescope == 'Gemini-South' :
        telescope = 'South'

    querystr = "(('UT Date'=\"%d\") AND ('Telescope'=\"%s\"))" % (t, telescope)

    error_message = ""
    result = []
    try:
        result = client.service.OpGetList(querystr, 0, 2)
    except WebFault, e:
        error_message = "WebFault: " + str(e.fault)
    except Exception, e:
        error_message = "Exception: " + str(e)

    if len(result) > 1:
        error_message = "Multiple Nightlog results returned from Remedy!"

    dict = {}
    if len(result):
        r = result[0]
        dict['T_night'] = r['Total_hours_available']
        dict['T_science'] = r['Total_hours_observed']
        dict['T_eng'] = (r['Commissioning'] + r['Telescope_shutdown'] + r['Engineering'])
        dict['T_fr'] = r['Total_hours_lost_faults']
        dict['T_weather'] = r['Total_hours_lost_weather']
    dict['error_message'] = error_message

    return dict

def total_seconds(td):
    """
    Utility function to calculate total seconds in a timedelta object
    """

    secs = td.seconds
    secs += td.days * 86400

    return secs
