"""
This module contains the QA metric database interface
"""

from sqlalchemy import desc
import urllib
import datetime
import time
import json
import math
import dateutil.parser
from ..gemini_metadata_utils import gemini_date, get_date_offset, gemini_daterange

from ..orm.qastuff import QAreport, QAmetricSB, QAmetricIQ, QAmetricZP, QAmetricPE
from ..orm.qastuff import evaluate_bg_from_metrics, evaluate_cc_from_metrics
from ..orm.diskfile import DiskFile
from ..orm.header import Header

from ..utils.web import get_context, Return

from . import templating

def qareport():
    """
    This function handles submission of QA metric reports via json
    """

    ctx = get_context()

    clientdata = ctx.raw_data
    #ctx.log("QAreport clientdata: %s" % clientdata)

    # We make here some reasonable assumptions about the input format
    if clientdata.startswith('['):
        thelist = parse_json(clientdata)
    else:
        ctx.resp.status = Return.HTTP_BAD_REQUEST
        return

    #ctx.log("thelist: %s" % thelist)

    qareport_ingest(thelist, submit_host=ctx.env.remote_host,
                    submit_time=datetime.datetime.now())

def qareport_ingest(thelist, submit_host=None, submit_time=datetime.datetime.now()):
    """
    This function takes a list of qareport dictionaries and inserts into the 
    database
    """

    session = get_context().session
    for qa_dict in thelist:
        # Get a new QAreport ORM object
        qareport = QAreport.from_dict(qa_dict, submit_host, submit_time)

        session.add(qareport)
        session.commit()

def parse_json(clientdata):
    """
    This function takes a string containg a json document containing a list of 
    qareports and makes it into a list of dictionaries.
    """
    return json.loads(clientdata)

@templating.templated("reports/qametrics.txt", content_type='text/plain',
                      with_generator=True)
def qametrics(metrics):
    """
    This function is the initial, simple display of QA metric data
    """

    def yield_metrics(cls):
        session = get_context().session
        query = session.query(cls).select_from(cls, QAreport).filter(cls.qareport_id == QAreport.id)
        for qa in query:
            hquery = session.query(Header).select_from(Header, DiskFile)\
                                .filter(Header.diskfile_id == DiskFile.id)\
                                .filter(DiskFile.canonical == True)\
                                .filter(Header.data_label == qa.datalabel)
            header = hquery.first()
            if header:
                filternm = header.filter_name
                utdt = header.ut_datetime
            else:
                filternm = None
                utdt = None

            yield filternm, utdt, qa

    pairs = (
        ('iq', QAmetricIQ),
        ('zp', QAmetricZP),
        ('sb', QAmetricSB),
        ('pe', QAmetricPE),
        )

    ret = {}
    for thing, cls in pairs:
        if thing in metrics:
            ret[thing] = yield_metrics(cls)

    return ret

def qaforgui(date):
    """
    This function outputs a JSON dump, aimed at feeding the QA metric GUI display
    You must pass a datestamp. It will only return results for datafiles from 
    that datestamp to 3 days later.
    """

    ctx = get_context()
    resp = ctx.resp

    try:
        if '-' in date:
            datestamp, enddatestamp = gemini_daterange(date,
                                                       offset=get_date_offset(),
                                                       as_datetime=True)
            assert enddatestamp > datestamp
        else:
            datestamp = gemini_date(date, offset=get_date_offset(),
                                    as_datetime=True)
            # Default 3 days worth for the gui;
            # stop the return getting huge over time
            window = datetime.timedelta(days=3)
            enddatestamp = datestamp+window
    except (AssertionError, TypeError, ValueError):
        resp.client_error(Return.HTTP_NOT_ACCEPTABLE, "Error: Invalid or null datestamp.")

    session = ctx.session
    resp.content_type = "application/json"

    # We only want the most recent of each value for each datalabel
    # Interested in IQ, ZP, BG

    # Get a list of datalabels
    def mquery(cls):
        return session.query(cls.datalabel).select_from(cls, Header)\
                    .filter(cls.datalabel == Header.data_label)\
                    .filter(Header.ut_datetime > datestamp)\
                    .filter(Header.ut_datetime < enddatestamp)

    datalabel_query = mquery(QAmetricIQ)\
                        .union(mquery(QAmetricZP))\
                        .union(mquery(QAmetricSB))\
                        .distinct()

    # Now loop through the datalabels.
    # For each datalabel, get the most recent QA measurement of each type. Only ones reported after the datestamp and before enddatestamp
    # Add the QA measurements to a list that we then dump out with json
    list_for_json = []
    # Comes back as a 1 element list, capture as such
    for (datalabel,) in datalabel_query:
        metadata = {'datalabel': datalabel}
        iq, cc, bg = {}, {}, {}
        submit_time_kludge = None

        # First try and find the header entry for this datalabel, and populate what comes from that
        query = session.query(Header).select_from(Header, DiskFile)\
                    .filter(Header.diskfile_id == DiskFile.id)\
                    .filter(DiskFile.canonical == True)\
                    .filter(Header.data_label == datalabel)
        header = query.first()
        # We can only populate the header info if it is in the header table
        # These items are used later in the code if available from the header,
        # init to None here
        airmass = None
        requested_iq = None
        requested_cc = None
        requested_bg = None

        if header:
            # These are not directly used as metadata items, but are used later
            # if available from the header
            try:
                airmass = float(header.airmass)
            except TypeError:
                # Will happen if header.airmass is None
                pass
            requested_iq = header.requested_iq
            requested_cc = header.requested_cc
            requested_bg = header.requested_bg

            # These are the metadata items we forward
            metadata.update({
                'raw_filename': header.diskfile.file.name,
                'ut_time': str(header.ut_datetime),
                'local_time': str(header.local_time),
                'wavelength': float(header.central_wavelength) if header.central_wavelength else None,
                'waveband': header.wavelength_band,
                'airmass': airmass,
                'filter': header.filter_name,
                'instrument': header.instrument,
                'object': header.object,
                # Parse the types string back into a list using a locked-down eval
                'types': eval(header.types, {"__builtins__":None}, {})
                })

        if (datestamp is None) or (header and (datestamp < header.ut_datetime < enddatestamp)):
            # Look for IQ metrics to report. Going to need to do the same merging trick here
            query = session.query(QAmetricIQ).select_from(QAmetricIQ, QAreport)\
                        .filter(QAmetricIQ.qareport_id == QAreport.id)\
                        .filter(QAmetricIQ.datalabel == datalabel)\
                        .order_by(desc(QAreport.submit_time))
            qaiq = query.first()

            # If we got anything, populate the iq dict
            if qaiq:
                submit_time_kludge = qaiq.qareport.submit_time
                iq = qaiq.to_evaluated_dict(airmass)
                try:
                    iq['requested'] = int(requested_iq)
                except TypeError:
                    pass

            # Look for CC metrics to report. The DB has the different detectors
            # in different entries, have to do some merging.
            # Find the qareport id of the most recent zp report for this datalabel
            query = session.query(QAreport).select_from(QAmetricZP, QAreport)\
                        .filter(QAmetricZP.qareport_id == QAreport.id)\
                        .filter(QAmetricZP.datalabel == datalabel)\
                        .order_by(desc(QAreport.submit_time))
            qarep = query.first()

            # Now find all the ZPmetrics for this qareport_id
            # By definition, it must be after the timestamp etc.
            if qarep:
                cc = evaluate_cc_from_metrics(qarep.zp_metrics)
                try:
                    cc['requested'] = int(requested_cc)
                except TypeError:
                    pass

                submit_time_kludge = qarep.submit_time


            # Look for BG metrics to report. The DB has the different detectors
            # in different entries, have to do some merging.
            # Find the qareport id of the most recent zp report for this datalabel
            query = session.query(QAreport).select_from(QAmetricSB, QAreport)\
                        .filter(QAmetricSB.qareport_id == QAreport.id)\
                        .filter(QAmetricSB.datalabel == datalabel)\
                        .order_by(desc(QAreport.submit_time))
            qarep = query.first()

            # Now find all the SBmetrics for this qareport_id
            # By definition, it must be after the timestamp etc.
            if qarep:
                bg = evaluate_bg_from_metrics(qarep.sb_metrics)

                try:
                    bg['requested'] = int(requested_bg)
                except TypeError:
                    pass

                submit_time_kludge = qarep.submit_time

            # Now, put the stuff we built into a dict that we can push out to json
            dct = {}
            if metadata:
                dct['metadata'] = metadata
            if iq:
                dct['iq'] = iq
            if cc:
                dct['cc'] = cc
            if bg:
                dct['bg'] = bg

            # Stuff in the extra stuff to keep adcc happy...
            dct['msgtype'] = 'qametric'
            try:
                dct['timestamp'] = float(submit_time_kludge.strftime("%s.%f"))
            except:
                dct['timestamp'] = 0.0

            # Add it to the json list, if there is anything
            if iq or cc or bg:
                list_for_json.append(dct)

    # Serialize it out via json to the request object
    resp.append_json(list_for_json, indent=4)
