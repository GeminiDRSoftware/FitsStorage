"""
This is a script which defines functions for generating content, general, and possibly usage statistics on the database. It queries the database for stats and outputs them as HTML via the apachehandler.
"""

from sqlalchemy import desc, func, join, and_, or_

from fits_storage_config import fits_system_status
from orm import sessionfactory
from orm.file import File
from orm.diskfile import DiskFile
from orm.header import Header
from orm.ingestqueue import IngestQueue

import apache_return_codes as apache

from datetime import datetime, timedelta, time as dt_time, date as dt_date
from collections import defaultdict, namedtuple

def stats(req):
    """
    Provides live statistics on fits database: total filesize, ingest queue status, and datarate for various date ranges is queried. Information is
    presented in html in the browser in a list format.
    """

    req.content_type = "text/html"
    req.write('<!DOCTYPE html><html><head>')
    req.write('<meta charset="UTF-8">')
    req.write("<title>FITS Storage database statistics</title></head>")
    req.write("<body>")
    req.write("<h1>FITS Storage database statistics</h1>")

    session = sessionfactory()
    try:

        # File table statistics
        query = session.query(File)
        req.write("<h2>File Table:</h2>")
        req.write("<ul>")
        req.write("<li>Total Rows: %d</li>" % query.count())
        req.write("</ul>")

        # DiskFile table statistics
        req.write("<h2>DiskFile Table:</h2>")
        req.write("<ul>")
        # Total rows
        query = session.query(DiskFile)
        totalrows = query.count()
        req.write("<li>Total Rows: %d</li>" % totalrows)
        # Present rows
        query = query.filter(DiskFile.present == True)
        presentrows = query.count()
        if totalrows != 0:
            percent = 100.0 * presentrows / totalrows
            req.write("<li>Present Rows: %d (%.2f %%)</li>" % (presentrows, percent))
        # Present size
        tpq = session.query(func.sum(DiskFile.file_size)).filter(DiskFile.present == True)
        tpsize = tpq.one()[0]
        if tpsize != None:
            req.write("<li>Total present size: %d bytes (%.02f GB)</li>" % (tpsize, (tpsize/1073741824.0)))
        # most recent entry
        query = session.query(func.max(DiskFile.entrytime))
        latest = query.one()[0]
        req.write("<li>Most recent diskfile entry was at: %s</li>" % latest)
        # Number of entries in last minute / hour / day
        periods = (('minute', timedelta(minutes=1)),
                   ('hour', timedelta(hours=1)),
                   ('day', timedelta(days=1)))
        for name, delta in periods:
            cnt = session.query(DiskFile)\
                            .filter(DiskFile.entrytime > (datetime.now() - delta))\
                            .count()
            req.write('<LI>Number of DiskFile rows added in the last %s: %d</LI>' % (name, cnt))
        # Last 10 entries
        query = session.query(DiskFile)\
                        .order_by(desc(DiskFile.entrytime))\
                        .limit(10)
        req.write('<LI>Last 10 diskfile entries added:<UL>')
        for i in query:
            req.write('<LI>%s : %s</LI>' % (i.file.name, i.entrytime))
        req.write('</UL></LI>')
        req.write("</ul>")

        # Header table statistics
        query = session.query(Header)
        req.write("<h2>Header Table:</h2>")
        req.write("<ul>")
        req.write("<li>Total Rows: %d</li>" % query.count())
        req.write("</ul>")

        # Ingest Queue Depth
        query = session.query(IngestQueue)
        req.write("<h2>Ingest Queue</h2>")
        req.write("<ul>")
        req.write("<li>Total Rows: %s</li>" % query.count())
        query = query.filter(IngestQueue.inprogress == True)
        req.write("<li>Currently In Progress: %s</li>" % query.count())
        req.write("</ul>")

        # Data rate statistics
        req.write("<h2>Data Rates</h2>")
        today = datetime.utcnow().date()
        zerohour = dt_time(0, 0, 0)
        comb = datetime.combine(today, zerohour)

        def print_stats(title, end, times, delta, short_message = False):
            start=end - delta
            req.write("<h3>%s</h3><ul>" % title)
            for i in range(times):
                query = session.query(func.sum(DiskFile.file_size), func.count(1))\
                                .select_from(join(Header, DiskFile))\
                                .filter(DiskFile.present == True)\
                                .filter(Header.ut_datetime >= start)\
                                .filter(Header.ut_datetime < end)
                bytes, count = query.one()
                if(not bytes):
                    bytes = 0
                    count = 0
                if short_message:
                    req.write("<li>%s: %.2f GB, %d files</li>" % (str(start.date()), bytes/1E9, count))
                else:
                    req.write("<li>%s - %s: %.2f GB, %d files</li>" % (str(start.date()), str(end.date()), bytes/1E9, count))
                start -= delta
                end -= delta
            req.write("</ul>")

        print_stats("Last 10 days", end=comb + timedelta(days=1),
                                    times=10,
                                    delta=timedelta(days=1),
                                    short_message = True)

        print_stats("Last 6 weeks", end=comb, times=6, delta=timedelta(days=7))
        print_stats("Last 6 pseudo-months", end=comb, times=6, delta=timedelta(days=30))

        req.write("</body></html>")

        return apache.HTTP_OK

    except IOError:
        pass
    finally:
        session.close()


def content(req):
    """
    Queries database for information concerning the total number and filesize of all stored files. Produces tables presenting the results, sorted by
    various properties such as instrument, observation class/type, and year of observation.
    """

    session = sessionfactory()

    req.content_type = "text/html"
    req.write('<!DOCTYPE html><html><head>')
    req.write('<meta charset="UTF-8">')
    req.write("<title>Database content statistics</title>")
    req.write('<link rel="stylesheet" href="/htmldocs/table.css">')
    req.write("</head><body>\n")

    if (fits_system_status == "development"):
        req.write('<h1>This is the development system, please use <a href="http://fits/">fits</a> for operational use</h1>')

    req.write("<h1>Database content statistics</h1>")

    # Presents total files and filesize
    query = session.query(func.count(), func.sum(DiskFile.file_size), func.sum(DiskFile.data_size)).filter(DiskFile.canonical == True)
    filenum, filesize, datasize = query.one()

    req.write("<p>Total number of files: %s</p>" % "{:,}".format(filenum))

    if filesize is not None:
        req.write("<p>Total file storage size: %s GB</p>" % '{:,.02f}'.format(filesize/1073741824.0))

    if datasize is not None:
        req.write("<p>Total FITS data size: %s GB</p>" % '{:,.02f}'.format(datasize/1073741824.0))

    req.write("<h3>Data and file volume by telescope/instrument</h3>")
    req.write("<TABLE>")
    req.write("<TR class=tr_head>")

    # build the telescope list
    query = session.query(Header.telescope).group_by(Header.telescope).order_by(Header.telescope)
    # results comes back as a list of one element tuples - clean up to a simple list
    tels = [tel for (tel,) in query if tel is not None]
    # tels is now a simple list with no None values.

    # Database content statistics
    # Populates table headers
    req.write('<TH rowspan="2">Telescope&nbsp;</TH>')
    req.write('<TH rowspan="2">Instrument&nbsp;</TH>')
    req.write('<TH colspan="2">Data Volume (GB)&nbsp;</TH>')
    req.write('<TH colspan="6">Number of files&nbsp;</TH>')
    req.write('</TR><TR class=tr_head>')
    req.write('<TH>Storage size&nbsp;</TH>')
    req.write('<TH>FITS filesize&nbsp;</TH>')
    req.write('<TH>Total&nbsp;</TH>')
    req.write('<TH>Engineering&nbsp;</TH>')
    req.write('<TH>Science&nbsp;</TH>')
    req.write('<TH>Science/Acq Obs. Class&nbsp;</TH>')
    req.write('<TH>Calibration Obs. Class&nbsp;</TH>')
    req.write('<TH>Object Obs. Type&nbsp;</TH>')
    req.write('</TR>')

    # This query takes canonical files for the grouped by telescope and instrument, and counts the
    # total files. This is the general query that we'll specialize to extract all the stats
    general_query = session.query(Header.telescope, Header.instrument, func.count()).select_from(join(DiskFile, Header))\
                                               .filter(DiskFile.canonical == True)\
                                               .filter(Header.telescope != None)\
                                               .filter(Header.instrument != None)\
                                               .group_by(Header.telescope, Header.instrument)

    # Specialized queries. Query objects are 'lazy', meaning that they will only be performed when we try to
    # extract rows from them
    filesizesq = general_query.add_columns(func.sum(DiskFile.file_size), func.sum(DiskFile.data_size))
    engq = general_query.filter(Header.engineering == True)
    sciq = general_query.filter(Header.engineering == False)
    classq = general_query.filter(or_(Header.observation_class == 'science',
                                          Header.observation_class=='acq'))
    calq = general_query.filter(or_(Header.observation_class=='progCal',
                                        Header.observation_class=='partnerCal',
                                        Header.observation_class=='acqCal',
                                        Header.observation_class=='dayCal'))
    typeq = general_query.filter(Header.observation_type == 'OBJECT')

    # Build up the results
    class Result(object):
        def __init__(self):
            self.instnum = 0
            self.instbytes = 0
            self.instdata = 0
            self.engnum = 0
            self.scinum = 0
            self.sciacqnum = 0
            self.calacqnum = 0
            self.objnum = 0

    results = defaultdict(Result)

    for tel, instr, cnt, fs, ds in filesizesq:
        obj = results[(tel, instr)]
        obj.instnum = cnt
        obj.instbytes = fs
        obj.instdata = ds

    pairs = ((engq, 'engnum'),
             (sciq, 'scinum'),
             (classq, 'sciacqnum'),
             (calq, 'calacqnum'),
             (typeq, 'objnum'))
    for qry, field in pairs:
        for tel, instr, cnt in qry:
            obj = results[(tel, instr)]
            setattr(obj, field, cnt)

    # Print out the results
    file_volume = "<TD>{}</TD><TD>{}</TD><TD>{:,.02f}</TD><TD>{:,.02f}</TD><TD>{:,}</TD><TD>{:,}</TD><TD>{:,}</TD><TD>{:,}</TD><TD>{:,}</TD><TD>{:,}</TD>"
    even = False
    for ((tel, instrument), values) in sorted(results.items()):
        even = not even
        req.write("<TR class=%s>" % ('tr_even' if even else 'tr_odd'))
        req.write(file_volume.format(tel, instrument, values.instbytes/1073741824.0,
                                                      values.instdata/1073741824.0,
                                                      values.instnum,
                                                      values.engnum,
                                                      values.scinum,
                                                      values.sciacqnum,
                                                      values.calacqnum,
                                                      values.objnum))

    req.write("</table>")

    # Database annual statistics
    req.write("<h3>Data and file volume by telescope/year</h3>")
    req.write("<table>")
    req.write("<TR class=tr_head>")

    # datetime variables and queries declared here
    # reject invalid 1969 type years by selecting post 1990
    firstyear = dt_date(1990, 01, 01)
    start = session.query(func.min(Header.ut_datetime)).filter(Header.ut_datetime > firstyear).first()[0]
    end = session.query(func.max(Header.ut_datetime)).first()[0]

    # Table headers
    req.write('<TH rowspan="2">Telescope&nbsp;</TH>')
    req.write('<TH rowspan="2">Year&nbsp;</TH>')
    req.write('<TH colspan="2">Data Volume (GB)&nbsp;</TH>')
    req.write('<TH rowspan="2">Number of files&nbsp;</TH>')
    req.write('</TR>')
    req.write('<TR class=tr_head>')
    req.write('<TH>Storage size&nbsp;</TH>')
    req.write('<TH>FITS filesize&nbsp;</TH>')
    req.write('</TR>')

    # Build the query (file size and num grouped by telescope and year)
    extract_year = func.extract('YEAR', Header.ut_datetime)
    yearquery = session.query(Header.telescope, extract_year, func.sum(DiskFile.file_size), func.count(), func.sum(DiskFile.data_size))\
                            .select_from(join(Header, DiskFile))\
                            .filter(DiskFile.canonical == True)\
                            .filter(Header.ut_datetime >= firstyear)\
                            .group_by(Header.telescope, extract_year)\
                            .order_by(desc(extract_year), Header.telescope)

    # Extract data
    results = defaultdict(dict)

    for tel, year, yearbytes, yearnum, yeardata in yearquery:
        if tel is None:
            continue
        results[year][tel] = (yearbytes, yearnum, yeardata)

    # Print out the data
    even = False
    for year, data in sorted(results.items(), reverse=True):
        for tel in tels:
            even = not even
            req.write("<TR class=%s>" % ('tr_even' if even else 'tr_odd'))
            req.write("<TD>%s</TD><TD>%d</TD>" % (tel, year))
            try:
                yearbytes, yearnum, yeardata = data[tel]
            except KeyError:
                continue
            if(yearbytes is not None and yearnum is not None and yeardata is not None):
                req.write("<TD>{:,.02f}</TD><TD>{:,.02f}</TD><TD>{:,}</TD>".format(yearbytes/1073741824.0,
                                                                                   yeardata/1073741824.0,
                                                                                   yearnum))

    req.write("</table>")
    req.write("</body>")
    req.write("</html>")

    return apache.HTTP_OK
