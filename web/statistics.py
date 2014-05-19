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
    req.write("<html>")
    req.write("<head><title>FITS Storage database statistics</title></head>")
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
    req.write('<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd"><html>')
    req.write("<head><title>Database content statistics</title>")
    req.write('<link rel="stylesheet" href="/htmldocs/table.css"></head>\n')
    req.write("<body>")
        
    if (fits_system_status == "development"):
        req.write('<h1>This is the development system, please use <a href="http://fits/">fits</a> for operational use</h1>')
        
    req.write("<h1>Database content statistics</h1>")
    
    # Presents total files and filesize
    req.write("<p>")
    query = session.query(func.count(), func.sum(DiskFile.file_size), func.sum(DiskFile.data_size)).filter(DiskFile.canonical == True)
    filenum = query.one()[0]
    filesize = query.one()[1]
    datasize = query.one()[2]

    req.write("<caption align=bottom>")
    req.write("Total number of files: %d</p>" % filenum)

    if filesize != None:
        req.write("<p>Total file storage size: %.02f GB</p>" % (filesize/1073741824.0))
    
    if datasize != None:
        req.write("<p>Total FITS data size: %.02f GB</p>" % (datasize/1073741824.0))

    req.write("<p>")
    req.write("<h3>Data and file volume by telescope/instrument</h3>")
    req.write("<TABLE border=0>")
    req.write("<TR class=tr_head>")
    even = 0
    
    #Database content statistics
    query = session.query(Header.telescope).group_by(Header.telescope).order_by(Header.telescope)
    tels = query.all()
    
    #Populates table headers
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

    #Loops through table headers and populates table
    for tel in tels:
        query = session.query(Header.instrument).group_by(Header.instrument).filter(Header.telescope == tel).order_by(Header.instrument)    
        instruments = query.all()
        for instrument in instruments:
            even = not even
                       
            if(even):
                cs = "tr_even"
            else:
                cs = "tr_odd"

            req.write("<TR class=%s>" % (cs))
        
            #this query gives file counts and filesize totals
            query = session.query(func.count(), func.sum(DiskFile.file_size), func.sum(DiskFile.data_size)).select_from(join(DiskFile, Header)).filter(DiskFile.canonical == True).filter(Header.telescope == tel).filter(Header.instrument == instrument)
                        
            #Telescope and instrument rows are populated here
            if tel[0] and instrument[0] != None:
                req.write("<TD>%s</TD>" % (str(tel[0])))
                req.write("<TD>%s</TD>" % (str(instrument[0])))
            
            #Instrument totals are tallied here
            instresult = query.one()
            instnum = instresult[0]
            instbytes = instresult[1]
            instdata = instresult[2]

            if tel[0] and instrument[0] and instdata and instbytes != None:
                req.write("<TD>%.02f GB</TD>" % (instbytes/1073741824.0))
                req.write("<TD>%.02f GB</TD>" % (instdata/1073741824.0))
                req.write("<TD>%d</TD>" % instnum)            

            #Engineering/Science totals are tallied here
            engquery = query.filter(Header.engineering == True)
            sciquery = query.filter(Header.engineering == False)
            engnum = engquery.one()[0]
            scinum = sciquery.one()[0]

            if engnum == None:
                req.write("<TD>0</TD>")
            else:
                req.write("<TD>%d</TD>" % engnum)

            if scinum == None:
                req.write("<TD>0</TD>")
            else:
                req.write("<TD>%d</TD>" % scinum)
            
            # Science+Acq row is populated here
            classquery = query.filter(or_(Header.observation_class == 'science', Header.observation_class=='acq'))
            classresult = classquery.one()
            classnum = classresult[0]
            
            if tel[0] and instrument[0] and classnum != None:
                req.write("<TD>%d</TD>" % classnum)
                                           
            # Calibration row is populated here
            calquery = query.filter(or_(Header.observation_class=='progCal', Header.observation_class=='partnerCal', Header.observation_class=='acqCal', Header.observation_class=='dayCal'))
            calresult = calquery.one()
            calnum = calresult[0]
            calbytes = calresult[1]

            if tel[0] and instrument[0] and calnum and calbytes != None:
                req.write("<TD>%d</TD>" % calnum)
             
            # Object files row is populated here
            typequery = query.filter(Header.observation_type == 'OBJECT')
            typeresult = typequery.one()
            typenum = typeresult[0]
            typebytes = typeresult[1]

            if tel[0] and instrument[0] and typenum and typebytes != None:
                req.write("<TD>%d</TD>" % typenum)
    
    req.write("</table></p>") 
   
    # Database annual statistics
    req.write("<h3>Data and file volume by telescope/year</h3>")
    req.write("<p>")
    req.write("<TABLE border=0>")
    req.write("<TR class=tr_head>")
    
    # datetime variables and queries declared here
    firstyear = datetime.date(1990, 01, 01)
    start = session.query(func.min(Header.ut_datetime)).filter(Header.ut_datetime > firstyear).first()[0]
    end = session.query(func.max(Header.ut_datetime)).first()[0]
    years = []
    
    if start and end != None:
        startyear = start.year
        endyear = end.year
        yearof = endyear

        while yearof >= startyear:
            years.append(yearof)
            yearof -= 1
            
    #Table headers
    req.write('<TH rowspan="2">Telescope&nbsp;</TH>')    
    req.write('<TH rowspan="2">Year&nbsp;</TH>')
    req.write('<TH colspan="2">Data Volume (GB)&nbsp;</TH>')
    req.write('<TH rowspan="2">Number of files&nbsp;</TH>')
    req.write('</TR>')
    req.write('<TR class=tr_head>')
    req.write('<TH>Storage size&nbsp;</TH>')
    req.write('<TH>FITS filesize&nbsp;</TH>')    
    req.write('</TR>')

    even = 0
    
    # iterates through datetimes and populates table
    for year in years:
        for tel in tels:
            even = not even
                        
            if(even):
                cs = "tr_even"
            else:
                cs = "tr_odd"
        
            req.write("<TR class=%s>" % (cs))
                
            if tel[0] and year != None:
                req.write("<TD>%s</TD>" % str(tel[0]))
                req.write("<TD>%s</TD>" % str(year))

            # queries for filesize and filenum in year that loop is currently accessing
            dateyearstart = datetime.datetime(year=(year-1), month=12, day=31)
            dateyearend = datetime.datetime(year=(year+1), month=01, day=01)
            yearquery = session.query(func.sum(DiskFile.file_size), func.count(), func.sum(DiskFile.data_size)).select_from(join(Header, DiskFile)).filter(DiskFile.canonical == True).filter(and_(Header.ut_datetime > dateyearstart, Header.ut_datetime < dateyearend))
            yearresult = yearquery.one()
            yearbytes = yearresult[0]
            yearnum = yearresult[1]
            yeardata = yearresult[2]
        
            if tel[0] and year and yearnum and yearbytes != None:
                req.write("<TD>%.02f GB</TD>" % (yearbytes/1073741824.0))
                req.write("<TD>%.02f GB</TD>" % (yeardata/1073741824.0))
                req.write("<TD>%d</TD>" % yearnum)

            req.write("</TR>")
    
    req.write("</table>")
    req.write("</p>")
    req.write("</body>")
    req.write("</html>")

    return apache.OK
