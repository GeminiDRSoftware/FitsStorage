"""
This module handles the web 'queue' functions. Mainly showing status
"""

from ..utils.queuestats import stats, error_summary, error_detail, UnknownQueueError
from ..orm import session_scope
from .user import needs_login

from mod_python import apache
from itertools import cycle

DETAIL_THRESHOLD = 20

page_body_tmpl = '''\
<html>
 <head>
  <title>{title}</title></head>
  <link rel="stylesheet" href="/table.css">
  <H1>{title}</H1>
 </head>
 <body>
{body}
 </body>
</html>
'''

general_status_tmpl = '''\
<table>
 <tr class='tr_head'><th>Queue<th># Pending<th># Errors</tr>
{rows}
</table>
<p>NB: The total number of elements in each queue is <em>pending + errors</em>

{detail}'''

general_status_row = "<tr class='{cls}'><td><strong>{name}</strong><td>{length}<td>{errors}</tr>"

detail_status_tmpl = '''\
<h2>Detailed errors for queue {name}</h2>
<table>
 <tr class='tr_head'><th>In queue since<th>Filename</tr>
{rows}
</table>'''

detail_status_row = "<tr class='{cls}'><td>{since}<td><a href='/queuestatus/{qname}/{oid}'>{filename}</a></tr>"

def row_cycle():
    return cycle(('tr_odd', 'tr_even'))

def queuestatus_summary(req):
    req.content_type = "text/html"

    with session_scope() as session:
        general_rows = []
        detail_tables = []
        for qstat, class_ in zip(stats(session), row_cycle()):
            general_rows.append(general_status_row.format(cls=class_, **qstat))
            nerr = qstat['errors']
            if nerr > 0:
                qname = qstat['name']
                if nerr > DETAIL_THRESHOLD:
                    qname = qname + ' (limited to the first {})'.format(DETAIL_THRESHOLD)
                summary = error_summary(session, qstat['type'], DETAIL_THRESHOLD)
                details = [detail_status_row.format(cls=dclass, qname=qstat['lname'], **error_desc)
                                for error_desc, dclass in zip(summary, row_cycle())]
                detail_tables.append(detail_status_tmpl.format(name=qname, rows='\n'.join(details)))

    body = general_status_tmpl.format(
        rows='\n'.join(general_rows),
        detail='\n'.join(detail_tables)
        )

    req.write(page_body_tmpl.format(title='Gemini Archive - Queue Status Page', body=body))

    return apache.HTTP_OK

error_detail_body = '''\
<table>
 <tr><td align='right'><strong>Filename:</strong><td>{filename}</tr>
 <tr><td align='right'><strong>Added:</strong><td>{since}</tr>
 <tr><td align='right' valign='top'><strong>Traceback:</strong></tr>
</table>
<pre>
{tb}
</pre>'''

def queuestatus_tb(req, qshortname, oid):
    req.content_type = "text/html"

    with session_scope() as session:
        det = error_detail(session, qshortname, oid)

    title = 'Gemini Archive - Error for object {oid} on the {qname} Queue'.format(oid=oid, **det)
    req.write(page_body_tmpl.format(title=title, body=error_detail_body.format(**det)))
    return apache.HTTP_OK

@needs_login(staffer=True)
def queuestatus(req, things):
    if len(things) > 1:
        try:
            return queuestatus_tb(req, things[0], int(things[1]))
        except (TypeError, ValueError):
            # things[1] is not a valid integer, thus not an ID...
            pass
        except UnknownQueueError:
            # Something failed under queuestatus_tb...
            pass
    return queuestatus_summary(req)
