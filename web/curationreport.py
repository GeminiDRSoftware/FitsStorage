"""
This module contains the curation_report html generator function. 
"""
from orm import sessionfactory

from orm.header import Header

from FitsStorageCuration import duplicate_datalabels, duplicate_canonicals, duplicate_present, present_not_canonical
        
import ApacheReturnCodes as apache

def curation_report(req, things):
    """
    Retrieves and prints out the desired values from the list created in 
    FitsStorageCuration.py
    """
    req.content_type = 'text/html'
    req.write('<html>')
    req.write('<head>')
    req.write('<title>FITS Storage database curation report</title><link rel="stylesheet" href="/htmldocs/table.css">')
    req.write('</head>')
    req.write('<body>')
    req.write('<h1>FITS Storage database curation report</h1>')

    session = sessionfactory()
    try:
        checkonly = None
        exclude = None
        if len(things) == 1:
            if things[0] == 'noeng':
                exclude = 'ENG'
            else:
                checkonly = things[0]
        if len(things) == 2:
            if things[0] == 'noeng':
                exclude = 'ENG'
                checkonly = things[1]
            elif things[1] == 'noeng':
                exclude = 'ENG'
                checkonly = things[0]


        # Work for duplicate_datalabels
        dupdata = duplicate_datalabels(session, checkonly, exclude)
        previous_ans = ''
        even = 0
        req.write('<h2>Duplicate Datalabel Rows:</h2>')
        if dupdata != []:
            # Write the table headers
            req.write('<table border=0><tr class=tr_head><th>DiskFile ID</th><th>FileName</th><th>DataLabel</th></tr>')
            # Makes a list of diskfile ids such that every duplicate found has only one diskfile id
            for val in dupdata:
                this_ans = val
                if previous_ans != this_ans:
                    header = session.query(Header).filter(Header.diskfile_id == this_ans).first()
                    # Writes out the row for every duplicate in html
                    if header:
                        even = not even
                        if(even):
                            req.write('<tr class=tr_even>')
                        else:
                            req.write('<tr class=tr_odd>')
                        req.write('<td>%s</td><td><a href="/summary/%s"> %s </a></td><td><a href="/summary/%s"> %s </a></td></tr>' %    (header.diskfile.id, header.diskfile.file.name, header.diskfile.file.name, header.data_label, header.data_label))                
                previous_ans = this_ans
            req.write('</table>')
        else:
            req.write("No rows with duplicate datalabels where canonical=True.<br/>")


        # Work for duplicate_canonicals
        dupcanon = duplicate_canonicals(session)
        previous_file = ''
        oneheader = 0
        empty = 0
        even = 0
        req.write('<h2>Duplicate Canonical Rows:</h2>')            
        # Makes a list of diskfile ids such that every duplicate row found has only one diskfile id
        for val in dupcanon:
            this_file = val.file_id
            if previous_file == this_file:
                # Writes the table headers
                if oneheader == 1:
                    pass
                else: 
                    req.write('<table border=0><tr class=tr_head><th>DiskFile id</th><th>FileName</th><th>Canonical</th></tr>')
                    oneheader += 1
                # Writes out the row for every duplicate in html
                even = not even
                if(even):
                    req.write('<tr class=tr_even>')
                else:
                    req.write('<tr class=tr_odd>')
                req.write('<td>%s</td><td><a href="/summary/%s"> %s </a></td><td>%s</td></tr>' %    (val.id, val.file.name, val.file.name, val.canonical))
                empty += 1 
            previous_file = this_file
        req.write('</table>')
        if empty == 0:
            req.write("No rows with duplicate file ids where canonical=True.<br/>")


        # Work for duplicate_present
        duppres = duplicate_present(session)
        previous_file = ''
        oneheader = 0
        empty = 0
        even = 0
        req.write('<h2>Duplicate Present Rows:</h2>')
        # Makes a list of diskfile ids such that every duplicate row found has only one diskfile id
        for val in duppres: 
            this_file = val.file_id
            if previous_file == this_file:
                # Writes the table headers
                if oneheader == 1:
                    pass
                else:
                    req.write('<table border=0><tr class=tr_head><th>DiskFile id</th><th>FileName</th><th>Present</th></tr>')
                    oneheader += 1
                # Writes out the row for every duplicate in html
                even = not even
                if(even):
                    req.write('<tr class=tr_even>')
                else:
                    req.write('<tr class=tr_odd>')
                req.write('<td>%s</td><td><a href="/summary/%s"> %s </a></td><td>%s</td></tr>' %    (val.id, val.file.name, val.file.name, val.present))
                empty += 1 
            previous_file = this_file
        req.write('</table>')
        if empty == 0:
            req.write("No rows with duplicate file ids where present=True.<br/>")


        # Work for present_not_canonical
        presnotcanon = present_not_canonical(session)
        previous_ans = ''
        even = 0
        req.write('<h2>Present Not Canonical Rows:</h2>')
        if presnotcanon != []:
            # Writes the table headers
            req.write('<table border=0><tr class=tr_head><th>DiskFile id</th><th>FileName</th><th>Present</th><th>Canonical</th></tr>')
            # Makes a list of diskfile ids such that every duplicate row found has only one diskfile id
            for val in presnotcanon:
                this_ans = val
                if previous_ans != this_ans:
                    # Writes out the row for every duplicate in html
                    even = not even
                    if(even):
                        req.write('<tr class=tr_even>')
                    else:
                        req.write('<tr class=tr_odd>')
                    req.write('<td>%s</td><td><a href="/summary/%s"> %s </a></td><td>%s</td><td>%s</td></tr>' %    (val.id, val.file.name, val.file.name, val.present, val.canonical))                 
                previous_ans = this_ans 
            req.write('</table>')
        else:
            req.write("No rows with the conditions present=True and canonical=False.<br/>")


        req.write('</body>')
        req.write('</html>')
        return apache.OK
    except IOError:
        pass
    finally:
        session.close()
    
