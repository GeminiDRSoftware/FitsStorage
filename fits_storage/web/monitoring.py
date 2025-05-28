"""
This module generates the instrument monitoring data exports
"""

from fits_storage.server.wsgi.context import get_context
from sqlalchemy import select
from fits_storage.server.orm.monitoring import Monitoring

def monitoring(thing):
    ctx = get_context()

    stmt = select(Monitoring)\
        .where(Monitoring.processing_tag == thing)\
        .order_by(Monitoring.filename)\
        .order_by(Monitoring.adid)

    # Get the keywords that we would like in the report
    keywords = ['OVERSCAN', 'OVERRMS', 'PIXMEAN', 'PIXSTDEV', 'PIXMED',
                'SNRMEAN', 'FSNRGT3']

    # We get one row (Monitoring instance) per keyword per ad_id per filename.
    # We need to combine all the keyword/values into one output row per
    # filename-label
    # We can do this in one pass through the query as we are ordering the
    # results by filename and ad_id, so we can watch for these changing to know
    # when to start a new result.
    # For now, we just build a results list in memory. We could (with care)
    # yield results as we build them.

    filename = None
    adid = None
    results = []
    result = {}
    for row in ctx.session.scalars(stmt):
        if row.filename != filename or row.adid != adid:
            if result:
                results.append(result)
            filename = row.filename
            adid = row.adid
            result = {'filename': filename, 'adid': adid, 'label': row.label,
                      'data_label': row.data_label,
                      'ut_datetime': row.ut_datetime,
                      # And the items we pull from header
                      'read_speed': row.header.detector_readspeed_setting,
                      'gain': row.header.detector_gain_setting,
                      'binning': row.header.detector_binning,
                      'roi': row.header.detector_roi_setting,
                      'qastate': row.header.qa_state
                      }
        # If we want this rows keyword, grab it now
        if row.keyword in keywords:
            result[row.keyword] = row.get_value()
    # When we fall out of the end of the loop, save the work in progress result
    if result:
        results.append(result)

    # Transmit the results
    items = ['filename', 'adid', 'data_label', 'ut_datetime',
             'label0', 'label1', 'label2', 'label3', 'label4', 'label5',
             'label6', 'label7', *keywords]
    ctx.resp.append('# ' +  '\t'.join(items) + '\n')
    for result in results:
        resultitems = [str(result[item]) for item in items]
        line = '\t'.join(resultitems)
        line += '\n'
        ctx.resp.append(line)

