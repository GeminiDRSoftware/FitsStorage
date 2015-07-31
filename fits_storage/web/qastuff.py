"""
This module contains the QA metric database interface
"""

from ..orm import sessionfactory
from sqlalchemy import desc
import urllib
import datetime
import time
import json
import math
import dateutil.parser
from ..apache_return_codes import HTTP_OK, HTTP_BAD_REQUEST, HTTP_NOT_ACCEPTABLE
from ..gemini_metadata_utils import gemini_date, get_date_offset

from ..orm.qastuff import QAreport, QAmetricSB, QAmetricIQ, QAmetricZP, QAmetricPE
from ..orm.diskfile import DiskFile
from ..orm.header import Header

def qareport(req):
    """
    This function handles submission of QA metric reports via json
    """

    if req.method == 'POST':
        clientdata = req.read()
        req.log_error("QAreport clientdata: %s" % clientdata)

        # We make here some reasonable assumptions about the input format
        if clientdata.startswith('['):
            thelist = parse_json(clientdata)
        else:
            return HTTP_BAD_REQUEST

        req.log_error("thelist: %s" % thelist)

        return qareport_ingest(thelist, submit_host=req.get_remote_host(), submit_time=datetime.datetime.now())
    else:
        return HTTP_NOT_ACCEPTABLE

def qareport_ingest(thelist, submit_host=None, submit_time=datetime.datetime.now()):
    """
    This function takes a list of qareport dictionaries and inserts into the database
    """
    session = sessionfactory()
    try:
        for qa_dict in thelist:
            # Get a new QAreport ORM object
            qareport = QAreport.from_dict(qa_dict, submit_host, submit_time)

            session.add(qareport)
            session.commit()
    finally:
        session.close()

    return HTTP_OK

def parse_json(clientdata):
    """
    This function takes a string containg a json document containing a list of qareports and
    makes it into a list of dictionaries.
    """
    return json.loads(clientdata)

def qametrics(req, things):
    """
    This function is the initial, simple display of QA metric data
    """
    session = sessionfactory()
    try:

        req.content_type = "text/plain"
        if 'iq' in things:
            query = session.query(QAmetricIQ).select_from(QAmetricIQ, QAreport).filter(QAmetricIQ.qareport_id == QAreport.id)
            qalist = query.all()

            req.write("#Datalabel, filename, detector, filter, utdatetime, Nsamples, FWHM, FWHM_std, isoFWHM, isoFWHM_std, "
                        "EE50d, EE50d_std, elip, elip_std, pa, pa_std, strehl, strehl_std, percentile_band, comments\n")

            for qa in qalist:
                hquery = session.query(Header).select_from(Header, DiskFile)
                hquery = hquery.filter(Header.diskfile_id == DiskFile.id).filter(DiskFile.canonical == True)
                hquery = hquery.filter(Header.data_label == qa.datalabel)
                header = hquery.first()
                if header:
                    filter = header.filter_name
                    utdt = header.ut_datetime
                else:
                    filter = None
                    utdt = None

                req.write("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s\n" % (
                            qa.datalabel, qa.filename, qa.detector, filter, utdt, qa.nsamples, qa.fwhm, qa.fwhm_std,
                            qa.isofwhm, qa.isofwhm_std, qa.ee50d, qa.ee50d_std, qa.elip, qa.elip_std, qa.pa, qa.pa_std,
                            qa.strehl, qa.strehl_std, qa.percentile_band, qa.comment))

            req.write("#---------")

        if 'zp' in things:
            query = session.query(QAmetricZP).select_from(QAmetricZP, QAreport).filter(QAmetricZP.qareport_id == QAreport.id)
            qalist = query.all()

            req.write("#Datalabel, filename, detector, filter, utdatetime, Nsamples, zp_mag, zp_mag_std, cloud, cloud_std, photref, percentile_band, comment\n")

            for qa in qalist:
                hquery = session.query(Header).select_from(Header, DiskFile)
                hquery = hquery.filter(Header.diskfile_id == DiskFile.id).filter(DiskFile.canonical == True)
                hquery = hquery.filter(Header.data_label == qa.datalabel)
                header = hquery.first()
                if header:
                    filter = header.filter_name
                    utdt = header.ut_datetime
                else:
                    filter = None
                    utdt = None

                req.write("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s\n" % (
                            qa.datalabel, qa.filename, qa.detector, filter, utdt, qa.nsamples, qa.mag, qa.mag_std,
                            qa.cloud, qa.cloud_std, qa.photref, qa.percentile_band, qa.comment))

            req.write("#---------")


        if 'sb' in things:
            query = session.query(QAmetricSB).select_from(QAmetricSB, QAreport).filter(QAmetricSB.qareport_id == QAreport.id)
            qalist = query.all()

            req.write("#Datalabel, filename, detector, filter, utdatetime, Nsamples, sb_mag, sb_mag_std, sb_electrons, sb_electrons_std, percentile_band, comment\n")

            for qa in qalist:
                hquery = session.query(Header).select_from(Header, DiskFile)
                hquery = hquery.filter(Header.diskfile_id == DiskFile.id).filter(DiskFile.canonical == True)
                hquery = hquery.filter(Header.data_label == qa.datalabel)
                header = hquery.first()
                if header:
                    filter = header.filter_name
                    utdt = header.ut_datetime
                else:
                    filter = None
                    utdt = None

                req.write("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s\n" % (
                            qa.datalabel, qa.filename, qa.detector, filter, utdt, qa.nsamples, qa.mag, qa.mag_std,
                            qa.electrons, qa.electrons_std, qa.percentile_band, qa.comment))

            req.write("#---------")

        if 'pe' in things:
            query = session.query(QAmetricPE).select_from(QAmetricPE, QAreport).filter(QAmetricPE.qareport_id == QAreport.id)
            qalist = query.all()

            req.write("#Datalabel, filename, detector, filter, utdatetime, Nsamples, dra, dra_std, ddec, ddec_std, astref, comment\n")

            for qa in qalist:
                hquery = session.query(Header).select_from(Header, DiskFile)
                hquery = hquery.filter(Header.diskfile_id == DiskFile.id).filter(DiskFile.canonical == True)
                hquery = hquery.filter(Header.data_label == qa.datalabel)
                header = hquery.first()
                if header:
                    filter = header.filter_name
                    utdt = header.ut_datetime
                else:
                    filter = None
                    utdt = None

                req.write("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s\n" % (
                            qa.datalabel, qa.filename, qa.detector, filter, utdt, qa.nsamples, qa.dra, qa.dra_std,
                            qa.ddec, qa.ddec_std, qa.astref, qa.comment))

            req.write("#---------")


    except IOError:
        pass
    finally:
        session.close()

    return HTTP_OK

def qaforgui(req, things):
    """
    This function outputs a JSON dump, aimed at feeding the QA metric GUI display
    You must pass a datestamp. It will only return results for datafiles from that datestamp
    to 3 days later.
    """
    datestamp = None
    try:
        datestamp = gemini_date(things[0], offset=get_date_offset(), as_datetime=True)
        # Default 3 days worth for the gui, to stop the return getting huge over time
        window = datetime.timedelta(days=3)
        enddatestamp = datestamp+window
    except (IndexError, ValueError):
        req.write("Error: no datestamp given")
        return HTTP_NOT_ACCEPTABLE

    session = sessionfactory()
    try:
        req.content_type = "application/json"

        # We only want the most recent of each value for each datalabel
        # Interested in IQ, ZP, BG

        # Get a list of datalabels
        iqquery = session.query(QAmetricIQ.datalabel).select_from(QAmetricIQ, Header).filter(QAmetricIQ.datalabel == Header.data_label)
        iqquery = iqquery.filter(Header.ut_datetime > datestamp).filter(Header.ut_datetime < enddatestamp)

        zpquery = session.query(QAmetricZP.datalabel).select_from(QAmetricZP, Header).filter(QAmetricZP.datalabel == Header.data_label)
        zpquery = zpquery.filter(Header.ut_datetime > datestamp).filter(Header.ut_datetime < enddatestamp)

        sbquery = session.query(QAmetricSB.datalabel).select_from(QAmetricSB, Header).filter(QAmetricSB.datalabel == Header.data_label)
        sbquery = sbquery.filter(Header.ut_datetime > datestamp).filter(Header.ut_datetime < enddatestamp)

        query = iqquery.union(zpquery).union(sbquery).distinct()
        datalabels = query.all()

        # Now loop through the datalabels.
        # For each datalabel, get the most recent QA measurement of each type. Only ones reported after the datestamp and before enddatestamp
        # Add the QA measurements to a list that we then dump out with json
        list_for_json = []
        for datalabel in datalabels:

            # Comes back as a 1 element list
            datalabel = datalabel[0]

            metadata = {}
            iq = {}
            cc = {}
            bg = {}
            metadata['datalabel'] = datalabel
            submit_time_kludge = None

            # First try and find the header entry for this datalabel, and populate what comes from that
            query = session.query(Header).select_from(Header, DiskFile).filter(Header.diskfile_id == DiskFile.id)
            query = query.filter(DiskFile.canonical == True).filter(Header.data_label == datalabel)
            header = query.first()
            # We can only populate the header info if it is in the header table
            # These items are used later in the code if available from the header, init to None here
            airmass = None
            requested_iq = None
            requested_cc = None
            requested_bg = None

            if header:
                # These are not directly used as metadata items, but are used later if available from the header
                if header.airmass is not None:
                    airmass = float(header.airmass)
                if header.requested_iq is not None:
                    requested_iq = header.requested_iq
                if header.requested_cc is not None:
                    requested_cc = header.requested_cc
                if header.requested_bg is not None:
                    requested_bg = header.requested_bg

                # These are the metadata items we forward
                metadata['raw_filename'] = header.diskfile.file.name
                metadata['ut_time'] = str(header.ut_datetime)
                metadata['local_time'] = str(header.local_time)
                metadata['wavelength'] = header.central_wavelength
                metadata['waveband'] = header.wavelength_band
                metadata['airmass'] = airmass
                metadata['filter'] = header.filter_name
                metadata['instrument'] = header.instrument
                metadata['object'] = header.object
                # Parse the types string back into a list using a locked-down eval
                metadata['types'] = eval(header.types, {"__builtins__":None}, {})

            if (datestamp is None) or (header and (header.ut_datetime > datestamp) and (header.ut_datetime < enddatestamp)):
                # Look for IQ metrics to report. Going to need to do the same merging trick here
                query = session.query(QAmetricIQ).select_from(QAmetricIQ, QAreport).filter(QAmetricIQ.qareport_id == QAreport.id)
                query = query.filter(QAmetricIQ.datalabel == datalabel)
                query = query.order_by(desc(QAreport.submit_time))
                qaiq = query.first()

                # If we got anything, populate the iq dict
                if qaiq:
                    iq['band'] = qaiq.percentile_band
                    iq['delivered'] = float(qaiq.fwhm) if qaiq.fwhm is not None else None
                    iq['delivered_error'] = float(qaiq.fwhm_std) if qaiq.fwhm_std is not None else None
                    if airmass is not None and qaiq.fwhm is not None:
                        iq['zenith'] = float(qaiq.fwhm) * airmass**(-0.6)
                        iq['zenith_error'] = float(qaiq.fwhm_std) * airmass**(-0.6)
                    else:
                        # Keep the gui happy by keeping the dictionary entries present
                        iq['zenith'] = None
                        iq['zenith_error'] = None
                    iq['ellipticity'] = float(qaiq.elip) if qaiq.elip is not None else None
                    iq['ellip_error'] = float(qaiq.elip_std) if qaiq.elip_std is not None else None
                    iq['comment'] = []
                    if len(qaiq.comment):
                        iq['comment'] = [qaiq.comment]
                    if requested_iq is not None:
                        iq['requested'] = int(requested_iq) 
                    submit_time_kludge = qaiq.qareport.submit_time
                    iq['adaptive_optics'] = bool(qaiq.adaptive_optics)
                    if iq['adaptive_optics']:
                        iq['ao_seeing'] = None
                        iq['ao_seeing_zenith'] = None
                        if qaiq.ao_seeing is not None:
                            iq['ao_seeing'] = float(qaiq.ao_seeing)
                            if airmass is not None:
                                iq['ao_seeing_zenith'] = float(qaiq.ao_seeing) * airmass**(-0.6)

                # Look for CC metrics to report. The DB has the different detectors in different entries, have to do some merging.
                # Find the qareport id of the most recent zp report for this datalabel
                query = session.query(QAreport).select_from(QAmetricZP, QAreport).filter(QAmetricZP.qareport_id == QAreport.id)
                query = query.filter(QAmetricZP.datalabel == datalabel)
                query = query.order_by(desc(QAreport.submit_time))
                qarep = query.first()

                # Now find all the ZPmetrics for this qareport_id
                # By definition, it must be after the timestamp etc.
                zpmetrics = []
                if qarep:
                    query = session.query(QAmetricZP).filter(QAmetricZP.qareport_id == qarep.id)
                    zpmetrics = query.all()

                    # Now go through those and merge them into the form required
                    # This is a bit tediouos, given that we may have a result that is split by amp,
                    # or we may have one from a mosaiced full frame image.
                    cc_band = []
                    cc_zeropoint = {}
                    cc_extinction = []
                    cc_extinction_error = []
                    cc_comment = []
                    for z in zpmetrics:
                        if z.percentile_band not in cc_band:
                            cc_band.append(z.percentile_band)
                        cc_extinction.append(float(z.cloud))
                        cc_extinction_error.append(float(z.cloud_std))
                        cc_zeropoint[z.detector] = {'value':float(z.mag), 'error':float(z.mag_std)}
                        if (z.comment not in cc_comment) and (len(z.comment)):
                            cc_comment.append(z.comment)

                    # Need to combine some of these to a single value to populate the cc dict
                    cc['band'] = ', '.join(cc_band)
                    cc['zeropoint'] = cc_zeropoint
                    if len(cc_extinction):
                        cc['extinction'] = sum(cc_extinction) / len(cc_extinction)

                        # Quick variance calculation, we could load numpy instead..
                        s = 0
                        for e in cc_extinction_error:
                            s += e*e
                        s /= len(cc_extinction_error)
                        cc['extinction_error'] = math.sqrt(s)

                    cc['comment'] = cc_comment
                    if requested_cc is not None:
                        cc['requested'] = int(requested_cc)

                    submit_time_kludge = qarep.submit_time


                # Look for BG metrics to report. The DB has the different detectors in different entries, have to do some merging.
                # Find the qareport id of the most recent zp report for this datalabel
                query = session.query(QAreport).select_from(QAmetricSB, QAreport).filter(QAmetricSB.qareport_id == QAreport.id)
                query = query.filter(QAmetricSB.datalabel == datalabel)
                query = query.order_by(desc(QAreport.submit_time))
                qarep = query.first()

                # Now find all the SBmetrics for this qareport_id
                # By definition, it must be after the timestamp etc.
                sbmetrics = []
                if qarep:
                    query = session.query(QAmetricSB).filter(QAmetricSB.qareport_id == qarep.id)
                    sbmetrics = query.all()

                    # Now go through those and merge them into the form required
                    # This is a bit tediouos, given that we may have a result that is split by amp,
                    # or we may have one from a mosaiced full frame image.
                    bg_band = []
                    bg_mag = []
                    bg_mag_std = []
                    bg_comment = []
                    for b in sbmetrics:
                        if b.percentile_band not in bg_band:
                            bg_band.append(b.percentile_band)
                        if b.mag is not None and b.mag_std is not None:
                            bg_mag.append(float(b.mag))
                            bg_mag_std.append(float(b.mag_std))
                        if (b.comment not in bg_comment) and (len(b.comment)):
                            bg_comment.append(b.comment)

                    # Need to combine some of these to a single value
                    if len(bg_band):
                        # Be aware of Nones - makes join choke
                        for i in range(len(bg_band)):
                            bg_band[i] = str(bg_band[i])
                        bg['band'] = ', '.join(bg_band)
                    if len(bg_mag):
                        bg['brightness'] = sum(bg_mag) / len(bg_mag)

                        # Quick variance calculation, we could load numpy instead..
                        s = 0
                        for e in bg_mag_std:
                            s += e*e
                        s /= len(bg_mag_std)
                        bg['brightness_error'] = math.sqrt(s)

                    bg['comment'] = bg_comment
                    if requested_bg is not None:
                        bg['requested'] = int(requested_bg)

                    submit_time_kludge = qarep.submit_time

                # Now, put the stuff we built into a dict that we can push out to json
                dict = {}
                if len(metadata):
                    dict['metadata'] = metadata
                if len(iq):
                    dict['iq'] = iq
                if len(cc):
                    dict['cc'] = cc
                if len(bg):
                    dict['bg'] = bg

                # Stuff in the extra stuff to keep adcc happy...
                dict['msgtype'] = 'qametric'
                try:
                    dict['timestamp'] = float(submit_time_kludge.strftime("%s.%f"))
                except:
                    dict['timestamp'] = 0.0

                # Add it to the json list, if there is anything
                if len(iq) or len(cc) or len(bg):
                    list_for_json.append(dict)

        # Serialze it out via json to the request object
        json.dump(list_for_json, req, indent=4)

    except IOError:
        pass
    finally:
        session.close()

    return HTTP_OK

