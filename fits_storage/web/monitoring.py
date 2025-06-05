"""
This module generates the instrument monitoring data exports
"""

from fits_storage.server.wsgi.context import get_context
from sqlalchemy import select
from fits_storage.server.orm.monitoring import Monitoring
from fits_storage.server.monitoring import report_keywords


def monitoring(things):
    ctx = get_context()

    report_type = None
    processing_tag = None
    for thing in things:
        if thing in report_keywords.keys():
            report_type = thing
        else:
            processing_tag = thing

    if report_type is None:
        ctx.resp.client_error(404, message="Could not determine report type")

    stmt = select(Monitoring)\
        .order_by(Monitoring.filename)\
        .order_by(Monitoring.adid)

    if processing_tag is not None:
        stmt = stmt.where(Monitoring.processing_tag == processing_tag)

    # This may need more thought, but for now, only return rows where the
    # filename contains the report_type (eg checkBias1 contains checkBias)
    stmt = stmt.where(Monitoring.filename.contains(report_type))

    # We get one row (Monitoring instance) per keyword per ad_id per filename.
    # We need to combine all the keyword/values into one output row per
    # filename-label
    # We can do this in one pass through the query as we are ordering the
    # results by filename and ad_id, so we can watch for these changing to know
    # when to start a new result.
    # For now, we just build a results list in memory. We could (with care)
    # yield results as we build them.

    keywords = report_keywords[report_type]
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
                      'recipe': row.recipe,
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
    items = ['filename', 'adid', 'data_label', 'ut_datetime', 'label',
             'read_speed','gain', 'binning', 'roi', 'qastate',
             *keywords]
    ctx.resp.append('# ' +  '\t'.join(items) + '\n')
    for result in results:
        resultitems = [str(result.get(item)) for item in items]
        line = '\t'.join(resultitems)
        line += '\n'
        ctx.resp.append(line)

