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

import datetime

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
        mbefore = datetime.datetime.now() - datetime.timedelta(minutes=1)
        hbefore = datetime.datetime.now() - datetime.timedelta(hours=1)
        dbefore = datetime.datetime.now() - datetime.timedelta(days=1)
        mcount = session.query(DiskFile).filter(DiskFile.entrytime > mbefore).count()
        hcount = session.query(DiskFile).filter(DiskFile.entrytime > hbefore).count()
        dcount = session.query(DiskFile).filter(DiskFile.entrytime > dbefore).count()
        req.write('<LI>Number of DiskFile rows added in the last minute: %d</LI>' % mcount)
        req.write('<LI>Number of DiskFile rows added in the last hour: %d</LI>' % hcount)
        req.write('<LI>Number of DiskFile rows added in the last day: %d</LI>' % dcount)
        # Last 10 entries
        query = session.query(DiskFile).order_by(desc(DiskFile.entrytime)).limit(10)
        list = query.all()
        req.write('<LI>Last 10 diskfile entries added:<UL>')
        for i in list:
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
        today = datetime.datetime.utcnow().date()
        zerohour = datetime.time(0, 0, 0)
        ddelta = datetime.timedelta(days=1)
        wdelta = datetime.timedelta(days=7)
        mdelta = datetime.timedelta(days=30)

        start = datetime.datetime.combine(today, zerohour)
        end = start + ddelta

        req.write("<h3>Last 10 days</h3><ul>")
        for i in range(10):
            query = session.query(func.sum(DiskFile.file_size), func.count(1)).select_from(join(Header, DiskFile)).filter(DiskFile.present == True).filter(Header.ut_datetime > start).filter(Header.ut_datetime < end)
            bytes, count = query.one()
            if(not bytes):
                bytes = 0
                count = 0
            req.write("<li>%s: %.2f GB, %d files</li>" % (str(start.date()), bytes/1E9, count))
            start -= ddelta
            end -= ddelta
        req.write("</ul>")

        end = datetime.datetime.combine(today, zerohour)
        start = end - wdelta
        req.write("<h3>Last 6 weeks</h3><ul>")
        for i in range(6):
            query = session.query(func.sum(DiskFile.file_size), func.count(1)).select_from(join(Header, DiskFile)).filter(DiskFile.present == True).filter(Header.ut_datetime > start).filter(Header.ut_datetime < end)
            bytes, count = query.one()
            if(not bytes):
                bytes = 0
                count = 0
            req.write("<li>%s - %s: %.2f GB, %d files</li>" % (str(start.date()), str(end.date()), bytes/1E9, count))
            start -= wdelta
            end -= wdelta
        req.write("</ul>")

        end = datetime.datetime.combine(today, zerohour)
        start = end - mdelta
        req.write("<h3>Last 6 pseudo-months</h3><ul>")
        for i in range(6):
            query = session.query(func.sum(DiskFile.file_size), func.count(1)).select_from(join(Header, DiskFile)).filter(DiskFile.present == True).filter(Header.ut_datetime > start).filter(Header.ut_datetime < end)
            bytes, count = query.one()
            if(not bytes):
                bytes = 0
                count = 0
            req.write("<li>%s - %s: %.2f GB, %d files</li>" % (str(start.date()), str(end.date()), bytes/1E9, count))
            start -= mdelta
            end -= mdelta
        req.write("</ul>")
        req.write("</body></html>")

        return apache.OK

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
    result = query.one()
    filenum = result[0]
    filesize = result[1]
    datasize = result[2]

    req.write("<p>Total number of files: %s</p>" % "{:,}".format(filenum))

    if filesize is not None:
        req.write("<p>Total file storage size: %s GB</p>" % '{:,.02f}'.format(filesize/1073741824.0))
    
    if datasize is not None:
        req.write("<p>Total FITS data size: %s GB</p>" % '{:,.02f}'.format(datasize/1073741824.0))

    req.write("<h3>Data and file volume by telescope/instrument</h3>")
    req.write("<TABLE>")
    req.write("<TR class=tr_head>")
    even = 0
    
    # Database content statistics

    # build the telescope list
    query = session.query(Header.telescope).group_by(Header.telescope).order_by(Header.telescope)
    results = query.all()
    # results comes back as a list of one element tuples - clean up to a simple list
    tels = []
    for result in results:
        if (result[0] is not None):
            tels.append(result[0])
    # tels is now a simple list with no None values.
    
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

    # Loops through table headers and populates table
    for tel in tels:
        # Build the instrument list
        query = session.query(Header.instrument).group_by(Header.instrument).filter(Header.telescope == tel).order_by(Header.instrument)    
        results = query.all()
        # results comes back as a list of one element tuples - clean up to a simple list
        instruments = []
        for result in results:
            if (result[0] is not None):
                instruments.append(result[0])
        # instruments is now a simple list with no None values

        for instrument in instruments:
            even = not even
                       
            if(even):
                cs = "tr_even"
            else:
                cs = "tr_odd"

            req.write("<TR class=%s>" % (cs))
        
            # this query gives file counts and filesize totals
            query = session.query(func.count(), func.sum(DiskFile.file_size), func.sum(DiskFile.data_size)).select_from(join(DiskFile, Header)).filter(DiskFile.canonical == True).filter(Header.telescope == tel).filter(Header.instrument == instrument)
                        
            # Telescope and instrument rows are populated here
            req.write("<TD>%s</TD>" % tel)
            req.write("<TD>%s</TD>" % instrument)
            
            # Instrument totals are tallied here
            instresult = query.one()
            instnum = instresult[0]
            instbytes = instresult[1]
            instdata = instresult[2]

            req.write("<TD>%s</TD>" % '{:,.02f}'.format(instbytes/1073741824.0))
            req.write("<TD>%s</TD>" % '{:,.02f}'.format(instdata/1073741824.0))
            req.write("<TD>%s</TD>" % '{:,}'.format(instnum))

            # this query gives only file counts
            query = session.query(func.count()).select_from(join(DiskFile, Header)).filter(DiskFile.canonical == True).filter(Header.telescope == tel).filter(Header.instrument == instrument)

            # Engineering/Science totals are tallied here
            engquery = query.filter(Header.engineering == True)
            sciquery = query.filter(Header.engineering == False)
            engnum = engquery.one()[0]
            scinum = sciquery.one()[0]

            if engnum == None:
                req.write("<TD>0</TD>")
            else:
                req.write("<TD>%s</TD>" % ("{:,}".format(engnum)))

            if scinum == None:
                req.write("<TD>0</TD>")
            else:
                req.write("<TD>%s</TD>" % ("{:,}".format(scinum)))
            
            # Science+Acq row is populated here
            classquery = query.filter(or_(Header.observation_class == 'science', Header.observation_class=='acq'))
            classresult = classquery.one()
            classnum = classresult[0]
            
            req.write("<TD>%s</TD>" % ("{:,}".format(classnum)))
                                           
            # Calibration row is populated here
            calquery = query.filter(or_(Header.observation_class=='progCal', Header.observation_class=='partnerCal', Header.observation_class=='acqCal', Header.observation_class=='dayCal'))
            calresult = calquery.one()
            calnum = calresult[0]

            req.write("<TD>%s</TD>" % ("{:,}".format(calnum)))
             
            # Object files row is populated here
            typequery = query.filter(Header.observation_type == 'OBJECT')
            typeresult = typequery.one()
            typenum = typeresult[0]

            req.write("<TD>%s</TD>" % ("{:,}".format(typenum)))
    
    req.write("</table>") 
   
    # Database annual statistics
    req.write("<h3>Data and file volume by telescope/year</h3>")
    req.write("<table>")
    req.write("<TR class=tr_head>")
    
    # datetime variables and queries declared here
    # reject invalid 1969 type years by selecting post 1990
    firstyear = datetime.date(1990, 01, 01)
    start = session.query(func.min(Header.ut_datetime)).filter(Header.ut_datetime > firstyear).first()[0]
    end = session.query(func.max(Header.ut_datetime)).first()[0]
    
    startyear = start.year
    endyear = end.year
    yearof = endyear

    # Build a list of years to show
    years = []
    while yearof >= startyear:
        years.append(yearof)
        yearof -= 1
            
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

    even = False
    
    # iterates through datetimes and populates table
    for year in years:
        for tel in tels:
            even = not even
                        
            if(even):
                cs = "tr_even"
            else:
                cs = "tr_odd"
        
            req.write("<TR class=%s>" % cs)
                
            req.write("<TD>%s</TD>" % tel)
            req.write("<TD>%d</TD>" % year)

            # queries for filesize and filenum in year that loop is currently accessing
            # make start and end of year datetime objects to compare against
            dateyearstart = datetime.datetime(year=year, month=01, day=01)
            dateyearend = datetime.datetime(year=(year+1), month=01, day=01)
            yearquery = session.query(func.sum(DiskFile.file_size), func.count(), func.sum(DiskFile.data_size)).select_from(join(Header, DiskFile)).filter(DiskFile.canonical == True).filter(Header.telescope == tel).filter(and_(Header.ut_datetime >= dateyearstart, Header.ut_datetime < dateyearend))
            yearresult = yearquery.one()
            yearbytes = yearresult[0]
            yearnum = yearresult[1]
            yeardata = yearresult[2]
        
            if(yearbytes is not None and yearnum is not None and yeardata is not None):
                req.write("<TD>%s</TD>" % '{:,.02f}'.format(yearbytes/1073741824.0))
                req.write("<TD>%s</TD>" % '{:,.02f}'.format(yeardata/1073741824.0))
                req.write("<TD>%s</TD>" % "{:,}".format(yearnum))

            req.write("</TR>")
    
    req.write("</table>")
    req.write("</body>")
    req.write("</html>")

    return apache.OK
