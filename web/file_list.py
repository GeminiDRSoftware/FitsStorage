"""
This is the Fits Storage Web Summary module. It provides the functions
which query the database and generate html for the web header
summaries.
"""
import json

from orm import sessionfactory
from web.summary import list_headers
from web.standards import xmlstandardobs
import apache_return_codes as apache

def xmlfilelist(req, selection):
    """
    This generates an xml list of the files that met the selection
    """
    req.content_type = "text/xml"
    req.write('<?xml version="1.0" ?>')
    req.write("<file_list>")
    req.write("<selection>%s</selection>" % selection)

    session = sessionfactory()
    orderby = ['filename_asc']
    try:
        headers = list_headers(session, selection, orderby)
        for header in headers:
            req.write("<file>")
            req.write("<name>%s</name>" % header.diskfile.file.name)
            req.write("<filename>%s</filename>" % header.diskfile.filename)
            req.write("<path>%s</path>" % header.diskfile.path)
            req.write("<compressed>%s</compressed>" % header.diskfile.compressed)
            req.write("<size>%d</size>" % header.diskfile.file_size)
            req.write("<file_size>%d</file_size>" % header.diskfile.file_size)
            req.write("<data_size>%d</data_size>" % header.diskfile.data_size)
            req.write("<md5>%s</md5>" % header.diskfile.file_md5)
            req.write("<file_md5>%s</file_md5>" % header.diskfile.file_md5)
            req.write("<data_md5>%s</data_md5>" % header.diskfile.data_md5)
            req.write("<lastmod>%s</lastmod>" % header.diskfile.lastmod)
            if header.phot_standard:
                xmlstandardobs(req, header.id)
            req.write("</file>")
    finally:
        session.close()
    req.write("</file_list>")
    return apache.OK


def jsonfilelist(req, selection):
    """
    This generates a JSON list of the files that met the selection
    """
    req.content_type = "application/json"

    session = sessionfactory()
    orderby = ['filename_asc']
    try:
        headers = list_headers(session, selection, orderby)
        thelist = []
        for header in headers:
            thedict = {}
            thedict['name'] = _for_json(header.diskfile.file.name)
            thedict['filename'] = _for_json(header.diskfile.filename)
            thedict['path'] = _for_json(header.diskfile.path)
            thedict['compressed'] = _for_json(header.diskfile.compressed)
            thedict['size'] = _for_json(header.diskfile.file_size)
            thedict['file_size'] = _for_json(header.diskfile.file_size)
            thedict['data_size'] = _for_json(header.diskfile.data_size)
            thedict['md5'] = _for_json(header.diskfile.file_md5)
            thedict['file_md5'] = _for_json(header.diskfile.file_md5)
            thedict['data_md5'] = _for_json(header.diskfile.data_md5)
            thedict['lastmod'] = _for_json(header.diskfile.lastmod)
            thelist.append(thedict)
    finally:
        session.close()
    json.dump(thelist, req, indent=4)
    return apache.OK

def jsonsummary(req, selection):
    """
    This generates a JSON list of the files that met the selection.
    This contains most of the details from the header table
    """
    req.content_type = "application/json"

    # Like the summaries, only list canonical files by default
    if 'canonical' not in selection.keys():
        selection['canonical']=True

    session = sessionfactory()
    orderby = ['filename_asc']
    try:
        headers = list_headers(session, selection, orderby)
        thelist = []
        for header in headers:
            thedict = {}
            thedict['name'] = _for_json(header.diskfile.file.name)
            thedict['filename'] = _for_json(header.diskfile.filename)
            thedict['path'] = _for_json(header.diskfile.path)
            thedict['compressed'] = _for_json(header.diskfile.compressed)
            thedict['size'] = _for_json(header.diskfile.file_size)
            thedict['file_size'] = _for_json(header.diskfile.file_size)
            thedict['data_size'] = _for_json(header.diskfile.data_size)
            thedict['md5'] = _for_json(header.diskfile.file_md5)
            thedict['file_md5'] = _for_json(header.diskfile.file_md5)
            thedict['data_md5'] = _for_json(header.diskfile.data_md5)
            thedict['lastmod'] = _for_json(header.diskfile.lastmod)
            thedict['program_id'] = _for_json(header.program_id)
            thedict['engineering'] = _for_json(header.engineering)
            thedict['science_verification'] = _for_json(header.science_verification)
            thedict['calibration_program'] = _for_json(header.calibration_program)
            thedict['observation_id'] = _for_json(header.observation_id)
            thedict['data_label'] = _for_json(header.data_label)
            thedict['telescope'] = _for_json(header.telescope)
            thedict['instrument'] = _for_json(header.instrument)
            thedict['ut_datetime'] = _for_json(header.ut_datetime)
            thedict['local_time'] = _for_json(header.local_time)
            thedict['observation_type'] = _for_json(header.observation_type)
            thedict['observation_class'] = _for_json(header.observation_class)
            thedict['object'] = _for_json(header.object)
            thedict['ra'] = _for_json(header.ra)
            thedict['dec'] = _for_json(header.dec)
            thedict['azimuth'] = _for_json(header.azimuth)
            thedict['elevation'] = _for_json(header.elevation)
            thedict['cass_rotator_pa'] = _for_json(header.cass_rotator_pa)
            thedict['airmass'] = _for_json(header.airmass)
            thedict['filter_name'] = _for_json(header.filter_name)
            thedict['exposure_time'] = _for_json(header.exposure_time)
            thedict['disperser'] = _for_json(header.disperser)
            thedict['camera'] = _for_json(header.camera)
            thedict['central_wavelength'] = _for_json(header.central_wavelength)
            thedict['wavelength_band'] = _for_json(header.wavelength_band)
            thedict['focal_plane_mask'] = _for_json(header.focal_plane_mask)
            thedict['detector_binning'] = _for_json(header.detector_binning)
            thedict['detector_config'] = _for_json(header.detector_config)
            thedict['detector_roi_setting'] = _for_json(header.detector_roi_setting)
            thedict['spectroscopy'] = _for_json(header.spectroscopy)
            thedict['mode'] = _for_json(header.mode)
            thedict['adaptive_optics'] = _for_json(header.adaptive_optics)
            thedict['laser_guide_star'] = _for_json(header.laser_guide_star)
            thedict['wavefront_sensor'] = _for_json(header.wavefront_sensor)
            thedict['gcal_lamp'] = _for_json(header.gcal_lamp)
            thedict['raw_iq'] = _for_json(header.raw_iq)
            thedict['raw_cc'] = _for_json(header.raw_cc)
            thedict['raw_wv'] = _for_json(header.raw_wv)
            thedict['raw_bg'] = _for_json(header.raw_bg)
            thedict['requested_iq'] = _for_json(header.requested_iq)
            thedict['requested_cc'] = _for_json(header.requested_cc)
            thedict['requested_wv'] = _for_json(header.requested_wv)
            thedict['requested_bg'] = _for_json(header.requested_bg)
            thedict['qa_state'] = _for_json(header.qa_state)
            thedict['release'] = _for_json(header.release)
            thedict['reduction'] = _for_json(header.reduction)
            thedict['types'] = _for_json(header.types)
            thedict['phot_standard'] = _for_json(header.phot_standard)
            
            thelist.append(thedict)
    finally:
        session.close()
    json.dump(thelist, req, indent=4)
    return apache.OK


from decimal import Decimal
from datetime import datetime, date, time
from sqlalchemy import Integer, Text, DateTime, Numeric, Boolean, Date, Time, BigInteger, Enum
def _for_json(thing):
    """
    JSON can't serialize some types, this does best representation we can
    """
    if isinstance(thing, Text) or isinstance(thing, datetime) or isinstance(thing, date) or isinstance(thing, time) or isinstance(thing, DateTime) or isinstance(thing, Date) or isinstance(thing, Time):
        return str(thing)
    if isinstance(thing, Integer) or isinstance(thing, BigInteger):
        return int(thing)
    if isinstance(thing, Numeric) or isinstance(thing, Decimal):
        return float(thing)
    if isinstance(thing, Enum):
        return str(thing)
    return thing
