"""
This module handles the web 'queue' functions. Mainly showing status
"""

from ..utils.queuestats import stats, error_summary
from ..orm import session_scope

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
 <tr class='tr_head'><th>Queue<th># Elements<th># Errors</tr>
{rows}
</table>

{detail}'''

general_status_row = "<tr class='{cls}'><td><strong>{name}</strong><td>{length}<td>{errors}</tr>"

detail_status_tmpl = '''\
<h2>Detailed errors for queue {name}</h2>
<table>
 <tr class='tr_head'><th>In queue since<th>Filename</tr>
{rows}
</table>'''

detail_status_row = "<tr class='{cls}'><td>{since}<td>{filename}</tr>"

def row_cycle():
    return cycle(('tr_odd', 'tr_even'))

def queuestatus(req):
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
                details = [detail_status_row.format(cls=dclass, **error_desc)
                                for error_desc, dclass in zip(summary, row_cycle())]
                detail_tables.append(detail_status_tmpl.format(name=qname, rows='\n'.join(details)))

    body = general_status_tmpl.format(
        rows='\n'.join(general_rows),
        detail='\n'.join(detail_tables)
        )

    req.write(page_body_tmpl.format(title='Gemini Archive - Queue Status Page', body=body))

    return apache.HTTP_OK
