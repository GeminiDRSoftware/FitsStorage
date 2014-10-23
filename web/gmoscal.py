"""
This module contains the gmoscal html generator function.
"""
import sqlalchemy
from sqlalchemy.sql.expression import cast
from sqlalchemy import func, join, desc
from orm import sessionfactory
from orm.gmos import Gmos
from orm.header import Header
from orm.diskfile import DiskFile
from orm.file import File

from web.selection import sayselection, queryselection
from web.calibrations import interval_hours
from cal import get_cal_object
from fits_storage_config import using_sqlite, fits_system_status, das_calproc_path

import apache_return_codes as apache

from math import fabs

import os
import copy
import datetime
import time
import re
import dateutil.parser


def gmoscal(req, selection):
    """
    This generates a GMOS imaging twilight flat, bias and nod and shuffle darks report.
    If no date or daterange is given, tries to find last processing date
    """

    title = "GMOS Cal (Imaging Twilight Flats and Biases) Report %s" % sayselection(selection)
    req.content_type = "text/html"
    req.write('<html><head><title>%s</title>' % title)
    req.write('<link rel="stylesheet" href="/htmldocs/table.css"></head><body><h1>%s</h1>' % title)
    if fits_system_status == 'development':
        req.write("<H1>This is the Development Server, not the operational system. If you're not sure why you're seeing this message, please consult PH</H1>")

    if using_sqlite:
        req.write("<H1>The GMOS Cal page is not implemented with the SQLite database backend as it uses database functionality not supported by SQLite.</H1>")
        req.write("<P>Talk to PH is you have a use case needing this.</P>")
        req.write("<P>You should not see this message from facility central servers</P>")
        return apache.HTTP_NOT_IMPLEMENTED

    # Get a database session
    session = sessionfactory()
    try:
        # First the Twilight Flats part
        req.write('<H2>Twilight Flats</H2>')

        # Was a date provided by user?
        datenotprovided = ('date' not in selection) and ('daterange' not in selection)
        # If no date or daterange, look on endor or josie to get the last processing date
        if datenotprovided:
            base_dir = das_calproc_path
            checkfile = 'Basecalib/flatall.list'
            enddate = datetime.datetime.now().date()
            oneday = datetime.timedelta(days=1)
            date = enddate
            found = -1000
            startdate = None
            while found < 0:
                datestr = date.strftime("%Y%b%d").lower()
                file = os.path.join(base_dir, datestr, checkfile)
                if os.path.exists(file):
                    found = 1
                    startdate = date
                date -= oneday
                found += 1

                if startdate:
                    # Start the day after the last reduction
                    startdate += oneday
                    selection['daterange'] = "%s-%s" % (startdate.strftime("%Y%m%d"), enddate.strftime("%Y%m%d"))
                    req.write("<H2>Auto-detecting Last Processing Date: %s</H2>" % selection['daterange'])

        # We do this twice, first for the science data, then for the twilight flat data
        # These are differentiated by being science or dayCal

        # Put the results into dictionaries, which we can then combine into one html table
        sci = {}
        tlf = {}
        for observation_class in (['science', 'dayCal']):

            # The basic query for this
            query = session.query(func.count(1), Header.filter_name, Header.detector_binning).select_from(join(join(DiskFile, File), Header))
            query = query.filter(DiskFile.canonical == True)

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
            query = query.filter(~Header.program_id.like('%ENG%'))

            # Group by clause
            query = query.group_by(Header.filter_name, Header.detector_binning).order_by(Header.detector_binning, Header.filter_name)

            list = query.all()


            # Populate the dictionary
            # as {'i-2x2':[10, 'i', '2x2'], ...}    ie [number, filter_name, binning]
            if observation_class == 'science':
                dict = sci
            else:
                dict = tlf

            for row in list:
                # row[0] = count, [1] = filter, [2] = binning
                key = "%s-%s" % (row[1], row[2])
                dict[key] = [row[0], row[1], row[2]]

        # Make the master dictionary
        # as {'i-2x2':[10, 20, 'i', '2x2'], ...}     [n_sci, n_tlf, filter_name, binning]
        all = {}
        for key in sci.keys():
            nsci = sci[key][0]
            ntlf = 0
            filter_name = sci[key][1]
            binning = sci[key][2]
            all[key] = [nsci, ntlf, filter_name, binning]
        for key in tlf.keys():
            if key in all.keys():
                all[key][1] = tlf[key][0]
            else:
                nsci = 0
                ntlf = tlf[key][0]
                filter_name = tlf[key][1]
                binning = tlf[key][2]
                all[key] = [nsci, ntlf, filter_name, binning]


        # Output the HTML table and links to summaries etc
        req.write('<TABLE border=0>')
        req.write('<TR class=tr_head>')
        req.write('<TH>Number of Science Frames</TH>')
        req.write('<TH>Number of Twilight Frames</TH>')
        req.write('<TH>Filter</TH>')
        req.write('<TH>Binning</TH>')
        req.write('</TR>')

        even = False
        keys = all.keys()
        keys.sort(reverse=True)
        for key in keys:
            even = not even
            if even:
                if (all[key][0] > 0) and (all[key][1] == 0):
                    cs = "tr_warneven"
                else:
                    cs = "tr_even"
            else:
                if (all[key][0] > 0) and (all[key][1] == 0):
                    cs = "tr_warnodd"
                else:
                    cs = "tr_odd"

            req.write("<TR class=%s>" % cs)

            for i in range(4):
                req.write("<TD>%s</TD>" % all[key][i])

            req.write("</TR>")
        req.write("</TABLE>")
        datething = ''
        if 'date' in selection:
            datething = selection['date']
        if 'daterange' in selection:
            datething = selection['daterange']
        req.write('<P><a href="/summary/GMOS/imaging/OBJECT/science/NotFail/%s">Science Frames Summary Table</a></P>' % datething)
        req.write('<P><a href="/summary/GMOS/imaging/OBJECT/dayCal/Lucky/%s">Twilight Flat Summary Table</a></P>' % datething)
        req.write('<P>NB. Summary tables will show ENG program data not reflected in the counts above.</P>')

        # Now the BIAS report
        req.write('<H2>Biases</H2>')

        # If no date or daterange, look on endor or josie to get the last processing date
        if datenotprovided:
            base_dir = das_calproc_path
            checkfile = 'Basecalib/biasall.list'
            enddate = datetime.datetime.now().date()
            oneday = datetime.timedelta(days=1)
            date = enddate
            found = -1000
            startdate = None
            while found < 0:
                datestr = date.strftime("%Y%b%d").lower()
                file = os.path.join(base_dir, datestr, checkfile)
                if os.path.exists(file):
                    found = 1
                    startdate = date
                date -= oneday
                found += 1

                if startdate:
                    # Start the day after the last reduction
                    startdate += oneday
                    selection['daterange'] = "%s-%s" % (startdate.strftime("%Y%m%d"), enddate.strftime("%Y%m%d"))
                    req.write("<H2>Auto-detecting Last Processing Date: %s</H2>" % selection['daterange'])

        if time.daylight:
            tzoffset = datetime.timedelta(seconds=time.altzone)
        else:
            tzoffset = datetime.timedelta(seconds=time.timezone)

        oneday = datetime.timedelta(days=1)
        offset = sqlalchemy.sql.expression.literal(tzoffset - oneday, sqlalchemy.types.Interval)
        query = session.query(func.count(1), cast((Header.ut_datetime + offset), sqlalchemy.types.DATE).label('utdate'), Header.detector_binning, Header.detector_roi_setting).select_from(join(join(DiskFile, File), Header))

        query = query.filter(DiskFile.canonical == True)

        # Fudge and add the selection criteria
        # Keep the same selection from the flats above, but drop the spectroscopy specifier and add some others
        selection.pop('spectroscopy')
        selection['observation_type'] = 'BIAS'
        selection['inst'] = 'GMOS'
        selection['qa_state'] = 'NotFail'
        query = queryselection(query, selection)

        query = query.group_by('utdate', Header.detector_binning, Header.detector_roi_setting).order_by('utdate', Header.detector_binning, Header.detector_roi_setting)

        list = query.all()

        # OK, re-organise results into tally table dict
        # dict is: {utdate: {binning: {roi: Number}}
        dict = {}
        for row in list:
            # Parse the element numbers for simplicity
            num = row[0]
            utdate = row[1]
            binning = row[2]
            roi = row[3]

            if utdate not in dict.keys():
                dict[utdate] = {}
            if binning not in dict[utdate].keys():
                dict[utdate][binning] = {}
            if roi not in dict[utdate][binning].keys():
                dict[utdate][binning][roi] = num

        # Output the HTML table
        # While we do it, add up the totals as a simple column tally
        binlist = ['1x1', '2x2', '2x1', '1x2', '2x4', '4x2', '4x1', '1x4', '4x4']
        roilist = ['Full Frame', 'Central Spectrum']
        req.write('<TABLE border=0>')
        req.write('<TR class=tr_head>')
        req.write('<TH rowspan=2>UT Date</TH>')
        for b in binlist:
            req.write('<TH colspan=2>%s</TH>' %b)
        req.write('</TR>')
        req.write('<TR class=tr_head>')
        for b in binlist:
            for r in roilist:
                req.write('<TH>%s</TH>'% r)
        req.write('</TR>')

        even = False
        utdates = dict.keys()
        utdates.sort(reverse=True)
        total = []
        for i in range(0, len(binlist)*len(roilist)):
            total.append(0)

        for utdate in utdates:
            even = not even
            if even:
                cs = "tr_even"
            else:
                cs = "tr_odd"

            req.write("<TR class=%s>" % cs)
            req.write("<TD>%s</TD>" % utdate)
            i = 0
            for b in binlist:
                for r in roilist:
                    try:
                        num = dict[utdate][b][r]
                    except KeyError:
                        num = 0
                    total[i] += num
                    i += 1
                    req.write("<TD>%d</TD>" % num)
            req.write("</TR>")

        req.write("<TR class=tr_head>")
        req.write("<TH>%s</TH>" % 'Total')
        for t in total:
            req.write("<TH>%d</TH>" % t)
        req.write("</TR>")
        req.write("</TABLE>")

        # OK, find if there were dates for which there were no biases...
        # Can only do this if we got a daterange selection, otherwise it's broken if there's none on the first or last day
        # utdates is a reverse sorted list for which there were biases.
        if 'daterange' in selection:
            # Parse the date to start and end datetime objects
            daterangecre = re.compile(r'(20\d\d[01]\d[0123]\d)-(20\d\d[01]\d[0123]\d)')
            m = daterangecre.match(selection['daterange'])
            startdate = m.group(1)
            enddate = m.group(2)
            tzoffset = datetime.timedelta(seconds=time.timezone)
            oneday = datetime.timedelta(days=1)
            startdt = dateutil.parser.parse("%s 14:00:00" % startdate)
            startdt = startdt + tzoffset - oneday
            enddt = dateutil.parser.parse("%s 14:00:00" % enddate)
            enddt = enddt + tzoffset - oneday
            enddt = enddt + oneday
            # Flip them round if reversed
            if startdt > enddt:
                tmp = enddt
                enddt = startdt
                startdt = tmp
            startdate = startdt.date()
            enddate = enddt.date()

            nobiases = []
            date = startdate
            while date <= enddate:
                if date not in utdates:
                    nobiases.append(str(date))
                date += oneday

            req.write('<P>There were %d dates with no biases not set to Fail: ' % len(nobiases))
            if len(nobiases) > 0:
                req.write(', '.join(nobiases))
            req.write('</P>')

        # Now the Nod and Shuffle report
        req.write('<H2>Nod and Shuffle Darks</H2>')
        req.write('<P>This table shows how many suitable N&S darks can be found for every nodandshuffle OBJECT science frame within the last year. It counts darks taken within 6 months of the science as well as the total number found. We aim to have 15 darks taken within 6 months of the science. You can also see the number of months between the science and the most distant one within the 15 to give you an idea how far back you have to go to find a set of 15. If you see the same observation Id listed twice, then there are observations in that observation ID that require different darks.</P>')

        # The basic query for this
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

        # Knock out ENG programs
        query = query.filter(~Header.program_id.like('%ENG%'))

        # Limit to things within 1 year
        now = datetime.datetime.now()
        year = datetime.timedelta(days=366)
        then = now - year
        query = query.filter(Header.ut_datetime > then)

        #query = query.group_by(Header.observation_id).order_by(Header.observation_id, desc(Header.ut_datetime))
        query = query.order_by(desc(Header.observation_id), desc(Header.ut_datetime))

        list = query.all()

        # OK, we're going to build the results table as a list of dictionaries first, so that we can group the obsIDs together
        # when we display the HTML.

        table = []

        for l in list:
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
            dict = {}
            dict['observation_id'] = l.observation_id
            dict['data_label'] = l.data_label
            dict['count'] = count
            dict['young'] = young
            dict['age'] = int(round(oldest/720, 1))
            table.append(dict)

        # Output the HTML table and links to summaries etc
        req.write('<TABLE border=0>')
        req.write('<TR class=tr_head>')
        req.write('<TH>Observation ID</TH>')
        req.write('<TH>Number Within 6 Months</TH>')
        req.write('<TH>Total Number known</TH>')
        req.write('<TH>Age of oldest one (months)</TH>')
        req.write('</TR>')

        even = True
        done = []
        for dict in table:
            nd = copy.copy(dict)
            del nd['data_label']
            if nd not in done:
                done.append(nd)
                even = not even
                if nd['young'] < 15:
                    if even:
                        cs = 'tr_warneven'
                    else:
                        cs = 'tr_warnodd'
                else:
                    if even:
                        cs = 'tr_even'
                    else:
                        cs = 'tr_odd'
                req.write("<TR class=%s>" % cs)
                req.write('<TD><a href="/summary/%s">%s</a></TD>' % (nd['observation_id'], nd['observation_id']))
                req.write("<TD>%d</TD>" % nd['young'])
                req.write("<TD>%d</TD>" % nd['count'])
                req.write("<TD>%d</TD>" % nd['age'])
                req.write("</TR>")

        req.write("</TABLE>")
        req.write("</body></html>")
        return apache.OK

    except IOError:
        pass
    finally:
        session.close()

